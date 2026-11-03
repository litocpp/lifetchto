module;
#include <rstd/enum.hpp>
module lifetchto.http;
import :tunnel;
import :backend;
import :connection;
import :state;
using namespace rstd::prelude;
using namespace rstd::literals;
namespace lifetchto::http
{
struct TunnelWrite {
    Bytes bytes;
    usize queued {}, confirmed {};
    bool  done {};
};
struct TunnelFields {
    Option<lihttpto::ResponseHead> head;
    Option<Error>                  error, cleanup_error;
    Option<ResourcePermit>         permit;
    Option<ConnectStage>           stage;
    u32                            stream_id {};
    Vec<Bytes>                     incoming;
    usize                          buffered {}, consumed {};
    Option<TunnelWrite>            write;
    bool reading {}, writing {}, stopping {}, failed {}, released {}, transferred {}, claimed {},
        remote_end {}, local_end {}, shutdown_requested {}, shutdown_done {}, graceful_reset {},
        close_taken {};
    void stop() {
        stopping = true;
        incoming = {};
        buffered = usize();
        if (released) permit = None();
    }
};
struct TunnelStreamState {
    rstd::sync::Mutex<TunnelFields> fields { TunnelFields {} };
    Event                           changed, connection_changed;
    lihttpto::h2::Request           request;
    Instant                         deadline;
    usize                           upload_window;
    u64                             session_id;
    Arc<BodyState>                  connection;
    u32                             id {};
    u64                             last_serial {};
    bool                            reset {};
    Option<Instant>                 stop_deadline;
    TunnelStreamState(lihttpto::h2::Request   request,
                      ResourcePermit          permit,
                      Event                   event,
                      const TransportOptions& options,
                      u64                     session_id,
                      Arc<BodyState>          connection)
        : connection_changed(rstd::move(event)),
          request(rstd::move(request)),
          deadline(Instant::now() + options.connect_timeout),
          upload_window(options.upload_window),
          session_id(session_id),
          connection(rstd::move(connection)) {
        fields.lock().unwrap()->permit = Some(rstd::move(permit));
    }
    auto progress() const -> ConnectProgress {
        Option<ConnectStage> stage;
        u32                  stream;
        {
            auto f = fields.lock().unwrap();
            stage  = f->stage;
            stream = f->stream_id;
        }
        return { stage.unwrap_or(connection->connect_stage()), session_id, stream };
    }
    void notify() const {
        changed.notify();
        connection_changed.notify();
    }
    void stop() const {
        {
            auto f = fields.lock().unwrap();
            f->stop();
        }
        notify();
    }
    void fail(Error error) const {
        auto context       = failure_progress(error, progress());
        context.session_id = session_id;
        {
            auto f            = fields.lock().unwrap();
            context.stream_id = f->stream_id;
        }
        auto annotated = connect_failure(rstd::move(error), context);
        {
            auto f = fields.lock().unwrap();
            if (! f->failed) {
                f->failed = true;
                f->error  = Some(rstd::move(annotated));
                f->stage  = Some(context.stage);
            }
            f->stopping = true;
        }
        notify();
    }
};
struct TunnelSessionFields {
    Vec<Arc<TunnelStreamState>> streams;
    bool                        closing {}, draining {}, done {};
    usize                       peer_limit;
    Vec<Error>                  errors;
    explicit TunnelSessionFields(usize limit): peer_limit(limit) {}
};
struct TunnelSession {
    u64                                    id;
    rstd::sync::Mutex<TunnelSessionFields> fields;
    Event                                  changed;
    Arc<BodyState>                         connecting { Arc<BodyState>::make(None<Instant>()) };
    Option<TaskControl>                    task;
    Option<ResourcePermit>                 permit;
    explicit TunnelSession(u64 id, usize limit, ResourcePermit permit)
        : id(id), fields(TunnelSessionFields(limit)), permit(Some(rstd::move(permit))) {}
    void stop() const {
        {
            auto f     = fields.lock().unwrap();
            f->closing = true;
            for (const auto& stream : f->streams) stream->stop();
        }
        connecting->cancel();
        changed.notify();
    }
};
struct TunnelPoolFields {
    Vec<Arc<TunnelSession>> sessions;
    Vec<Error>              errors;
    usize                   handles { 1 };
    u64                     next_session_id { 1 };
    bool                    closed {};
};
struct TunnelPool {
    NativeBackend                       backend;
    Arc<NativeState>                    native;
    ResourceContext                     resources;
    lihttpto::Url                       url;
    RouteChoice                         route;
    rstd::sync::Mutex<TunnelPoolFields> fields { TunnelPoolFields {} };
    TunnelPool(NativeBackend    backend,
               Arc<NativeState> native,
               ResourceContext  resources,
               lihttpto::Url    url,
               RouteChoice      route)
        : backend(rstd::move(backend)),
          native(rstd::move(native)),
          resources(rstd::move(resources)),
          url(rstd::move(url)),
          route(rstd::move(route)) {}
    void stop() const {
        auto f    = fields.lock().unwrap();
        f->closed = true;
        for (const auto& session : f->sessions) session->stop();
    }
};
auto tunnel_io_error(Error error) -> rstd::io::Error {
    if (error.is_Connect()) {
        auto& cause = error.as_Connect().cause;
        if (error.context().is_none()) {
            return rstd::move(cause);
        }
        auto kind = cause.kind();
        return rstd::io::Error::with_source(
            kind, Box<rstd::dyn<rstd::error::Error>>::make(rstd::move(error)));
    }
    auto kind = rstd::io::ErrorKind::ConnectionReset;
    if (error.is_Timeout())
        kind = rstd::io::ErrorKind::TimedOut;
    else if (error.is_Canceled())
        kind = rstd::io::ErrorKind::Interrupted;
    else if (error.is_Closed())
        kind = rstd::io::ErrorKind::NotConnected;
    else if (error.is_Busy())
        kind = rstd::io::ErrorKind::ResourceBusy;
    else if (error.is_Capacity())
        kind = rstd::io::ErrorKind::OutOfMemory;
    return rstd::io::Error::with_source(
        rstd::io::ErrorKind { kind }, Box<rstd::dyn<rstd::error::Error>>::make(rstd::move(error)));
}
struct TunnelOperation {
    Arc<TunnelStreamState> stream;
    bool                   read {}, armed { true }, stop_on_drop { true };
    ~TunnelOperation() {
        {
            auto f = stream->fields.lock().unwrap();
            if (read)
                f->reading = false;
            else
                f->writing = false;
            if (armed && stop_on_drop) f->stop();
        }
        stream->notify();
    }
};
auto tunnel_read(Arc<TunnelPool> pool, Arc<TunnelStreamState> stream, rstd::bytes::BytesMut buffer)
    -> coro<rstd::async::io::StreamReceive> {
    (void)pool;
    buffer.clear();
    {
        auto f = stream->fields.lock().unwrap();
        if (f->reading)
            co_return rstd::async::io::StreamReceive {
                Err(rstd::io::Error::from_kind(
                    rstd::io::ErrorKind { rstd::io::ErrorKind::ResourceBusy })),
                rstd::move(buffer)
            };
        f->reading = true;
    }
    TunnelOperation operation { stream.clone(), true, true, false };
    for (;;) {
        auto event = stream->changed.listen();
        {
            auto f = stream->fields.lock().unwrap();
            if (! f->incoming.is_empty()) {
                auto& bytes = f->incoming[usize()];
                auto  count = rstd::min(buffer.remaining_mut(), bytes.len());
                buffer.put_slice(bytes.slice(usize(), count).as_slice());
                bytes.advance(count);
                if (bytes.is_empty()) f->incoming.remove(usize());
                f->buffered -= count;
                f->consumed += count;
                if (f->released && f->buffered == usize()) f->permit = None();
                operation.armed = false;
                stream->connection_changed.notify();
                co_return rstd::async::io::StreamReceive { Ok(count), rstd::move(buffer) };
            }
            if (f->error.is_some()) {
                operation.armed = false;
                co_return rstd::async::io::StreamReceive {
                    Err(tunnel_io_error(f->error.take().unwrap())), rstd::move(buffer)
                };
            }
            if (f->stopping) {
                operation.armed = false;
                co_return rstd::async::io::StreamReceive {
                    Err(rstd::io::Error::from_kind(
                        rstd::io::ErrorKind { rstd::io::ErrorKind::Interrupted })),
                    rstd::move(buffer)
                };
            }
            if (f->remote_end || buffer.remaining_mut() == usize()) {
                operation.armed = false;
                co_return rstd::async::io::StreamReceive { Ok(usize()), rstd::move(buffer) };
            }
        }
        co_await rstd::move(event);
    }
}
auto tunnel_send(Arc<TunnelPool> pool, Arc<TunnelStreamState> stream, Bytes bytes)
    -> coro<rstd::io::Result<usize>> {
    (void)pool;
    {
        auto f = stream->fields.lock().unwrap();
        if (f->writing)
            co_return Err(rstd::io::Error::from_kind(
                rstd::io::ErrorKind { rstd::io::ErrorKind::ResourceBusy }));
        if (f->failed) co_return Err(tunnel_io_error(Error::Closed()));
        if (f->stopping || f->local_end || f->shutdown_requested)
            co_return Err(rstd::io::Error::from_kind(
                rstd::io::ErrorKind { rstd::io::ErrorKind::BrokenPipe }));
        if (bytes.is_empty()) co_return Ok(usize());
        f->writing = true;
        auto count = rstd::min(bytes.len(), stream->upload_window);
        f->write   = Some(TunnelWrite { bytes.slice(usize(), count) });
    }
    TunnelOperation operation { stream.clone(), false };
    stream->connection_changed.notify();
    for (;;) {
        auto event = stream->changed.listen();
        {
            auto  f     = stream->fields.lock().unwrap();
            auto& write = *f->write;
            if (write.done || f->released) {
                auto count = write.confirmed;
                auto error = f->failed
                                 ? tunnel_io_error(Error::Closed())
                                 : rstd::io::Error::from_kind(rstd::io::ErrorKind {
                                       f->graceful_reset ? rstd::io::ErrorKind::BrokenPipe
                                                         : rstd::io::ErrorKind::Interrupted });
                if (write.done || f->released) f->write = None();
                operation.armed = false;
                if (count != usize()) co_return Ok(count);
                co_return Err(rstd::move(error));
            }
        }
        co_await rstd::move(event);
    }
}
auto tunnel_flush(Arc<TunnelPool> pool, Arc<TunnelStreamState> stream, bool shutdown)
    -> coro<rstd::io::Result<empty>> {
    (void)pool;
    {
        auto f = stream->fields.lock().unwrap();
        if (f->writing)
            co_return Err(rstd::io::Error::from_kind(
                rstd::io::ErrorKind { rstd::io::ErrorKind::ResourceBusy }));
        if (f->failed) co_return Err(tunnel_io_error(Error::Closed()));
        if (f->stopping)
            co_return Err(rstd::io::Error::from_kind(
                rstd::io::ErrorKind { rstd::io::ErrorKind::Interrupted }));
        if (f->graceful_reset) {
            if (f->released) co_return Ok(empty {});
        } else {
            if (! shutdown || f->shutdown_done) co_return Ok(empty {});
            f->shutdown_requested = true;
        }
        f->writing = true;
    }
    TunnelOperation operation { stream.clone(), false };
    stream->connection_changed.notify();
    for (;;) {
        auto event = stream->changed.listen();
        {
            auto f = stream->fields.lock().unwrap();
            if (f->failed) {
                operation.armed = false;
                co_return Err(tunnel_io_error(Error::Closed()));
            }
            if (f->stopping) {
                operation.armed = false;
                co_return Err(rstd::io::Error::from_kind(
                    rstd::io::ErrorKind { rstd::io::ErrorKind::Interrupted }));
            }
            if (f->graceful_reset ? f->released : f->shutdown_done) {
                operation.armed = false;
                co_return Ok(empty {});
            }
        }
        co_await rstd::move(event);
    }
}
auto tunnel_close(Arc<TunnelStreamState> stream) -> coro<Result<empty>> {
    stream->stop();
    for (;;) {
        auto event = stream->changed.listen();
        {
            auto f = stream->fields.lock().unwrap();
            if (f->released) {
                if (f->close_taken)
                    co_return Err(Error::Contract("tunnel close result already taken"_Str));
                f->close_taken = true;
                if (f->cleanup_error.is_some()) co_return Err(f->cleanup_error.take().unwrap());
                co_return Ok(empty {});
            }
        }
        co_await rstd::move(event);
    }
}
auto tunnel_stream_wait_closed(Arc<TunnelPool> pool, Arc<TunnelStreamState> stream)
    -> coro<rstd::io::Result<empty>> {
    (void)pool;
    stream->stop();
    for (;;) {
        auto event = stream->changed.listen();
        {
            auto fields = stream->fields.lock().unwrap();
            if (fields->released) co_return Ok(empty {});
        }
        co_await rstd::move(event);
    }
}
struct TunnelStream {
    Arc<TunnelPool>        pool;
    Arc<TunnelStreamState> stream;
    auto                   recv_into(rstd::bytes::BytesMut buffer) const {
        return tunnel_read(pool.clone(), stream.clone(), rstd::move(buffer));
    }
    auto send(Bytes bytes) const {
        return tunnel_send(pool.clone(), stream.clone(), rstd::move(bytes));
    }
    auto flush() const { return tunnel_flush(pool.clone(), stream.clone(), false); }
    auto shutdown_write() const { return tunnel_flush(pool.clone(), stream.clone(), true); }
    void request_stop() const { stream->stop(); }
    auto wait_closed() const { return tunnel_stream_wait_closed(pool.clone(), stream.clone()); }
    auto take_close_result() const -> Option<rstd::io::Result<empty>> {
        auto fields = stream->fields.lock().unwrap();
        if (! fields->released || fields->close_taken) return None();
        fields->close_taken = true;
        if (fields->cleanup_error)
            return Some(rstd::io::Result<empty>(
                Err(tunnel_io_error(fields->cleanup_error.take().unwrap()))));
        return Some(rstd::io::Result<empty>(Ok(empty {})));
    }
};
struct TunnelPoolOwner {
    Arc<TunnelPool> pool;
    void            request_stop(StopReason) const { pool->stop(); }
    auto            release(ReleaseContext) const -> coro<Result<empty>> {
        pool->stop();
        Vec<Arc<TunnelSession>> sessions;
        {
            auto f = pool->fields.lock().unwrap();
            for (const auto& s : f->sessions) sessions.push(s.clone());
        }
        Vec<Error> errors;
        {
            auto f = pool->fields.lock().unwrap();
            errors = rstd::move(f->errors);
        }
        for (const auto& session : sessions) {
            if (session->task.is_some()) {
                auto result = co_await session->task->wait();
                // Task failures are collected by the task's registration scope.
            }
            auto f = session->fields.lock().unwrap();
            for (auto& error : f->errors) errors.push(rstd::move(error));
            f->errors.clear();
        }
        auto closed = co_await pool->backend.close();
        if (closed.is_err()) errors.push(rstd::move(closed).unwrap_err());
        co_return collect_errors(rstd::move(errors));
    }
};
using TunnelRead  = coro<Result<Bytes>>::Future;
using TunnelSend  = coro<Result<usize>>::Future;
using TunnelTimer = decltype(rstd::async::into_future(rstd::async::sleep(Duration {})));
struct TunnelIo {
    Option<TunnelRead>    read;
    Option<TunnelSend>    write;
    Option<Result<Bytes>> received;
    Option<Result<usize>> written;
};
struct TunnelWake {
    TunnelIo*           io;
    EventWait           changed;
    Option<TunnelTimer> timer;
    using Output = empty;
    auto poll(mut_ref<TunnelWake> self, rstd::task::Context& cx) -> rstd::task::Poll<empty> {
        bool ready = false;
        if (self->io->read.is_some()) {
            auto next = rstd::future::poll(*self->io->read, cx);
            if (next.is_ready()) {
                self->io->received = Some(rstd::move(next).take());
                self->io->read     = None();
                ready              = true;
            }
        }
        if (self->io->write.is_some()) {
            auto next = rstd::future::poll(*self->io->write, cx);
            if (next.is_ready()) {
                self->io->written = Some(rstd::move(next).take());
                self->io->write   = None();
                ready             = true;
            }
        }
        if (rstd::future::poll(self->changed, cx).is_ready()) ready = true;
        if (self->timer.is_some() && rstd::future::poll(*self->timer, cx).is_ready()) ready = true;
        return ready ? rstd::task::Poll<empty>::Ready(empty {})
                     : rstd::task::Poll<empty>::Pending();
    }
};
struct TunnelWire {
    lihttpto::h2::Outbound output;
    u64                    serial;
    usize                  offset {};
};
auto tunnel_driver(Arc<TunnelPool> pool, Arc<TunnelSession> owner) -> coro<empty>;
struct TunnelTask {
    rstd::sync::Weak<TunnelPool>    pool;
    rstd::sync::Weak<TunnelSession> session;
    auto                            run(Cancellation) -> coro<Result<empty>> {
        auto p = pool.upgrade();
        auto s = session.upgrade();
        if (p && s) co_await tunnel_driver(rstd::move(p), rstd::move(s));
        co_return Ok(empty {});
    }
    void request_stop(StopReason) const {
        auto s = session.upgrade();
        if (s) s->stop();
    }
};
} // namespace lifetchto::http
namespace rstd
{
template<>
struct Impl<async::io::CompletionStream, lifetchto::http::TunnelStream>
    : ImplBase<lifetchto::http::TunnelStream> {
    auto recv_into(bytes::BytesMut buffer) const {
        return this->self().recv_into(rstd::move(buffer));
    }
    auto send(bytes::Bytes bytes) const { return this->self().send(rstd::move(bytes)); }
    auto flush() const { return this->self().flush(); }
    auto shutdown_write() const { return this->self().shutdown_write(); }
    void request_stop() const { this->self().request_stop(); }
    auto wait_closed() const { return this->self().wait_closed(); }
    auto take_close_result() const { return this->self().take_close_result(); }
};
template<>
struct Impl<lifetchto::ManagedResource, lifetchto::http::TunnelPoolOwner>
    : ImplBase<lifetchto::http::TunnelPoolOwner> {
    void request_stop(lifetchto::StopReason reason) const { this->self().request_stop(reason); }
    auto release(lifetchto::ReleaseContext context) const { return this->self().release(context); }
};
} // namespace rstd
namespace lifetchto::http
{
auto tunnel_driver(Arc<TunnelPool> pool, Arc<TunnelSession> owner) -> coro<empty> {
    auto slot = Arc<Slot>::make(pool->url.http_origin()->serialize(), pool->route.route.clone());
    auto connected = co_await connect_to(
        pool->native.clone(), slot.clone(), pool->url, owner->connecting.clone());
    Option<Connection> native;
    Option<Error>      fatal;
    if (connected.is_err())
        fatal = Some(rstd::move(connected).unwrap_err());
    else
        native = Some(rstd::move(connected).unwrap());
    if (native.is_some() && ! native->http2)
        fatal =
            Some(connect_failure(Error::Unsupported("CONNECT tunnels require negotiated h2"_Str),
                                 ConnectProgress { ConnectStage::Tls, owner->id }));
    if (fatal.is_none()) owner->connecting->set_connect_stage(ConnectStage::Http2Settings);
    lihttpto::h2::Limits limits;
    limits.header_bytes          = pool->native->options.head_limit;
    limits.maximum_streams       = pool->native->options.max_http2_streams;
    limits.receive_window        = pool->native->options.receive_window;
    limits.maximum_informational = pool->native->options.max_informational;
    auto created                 = lihttpto::h2::ClientConnection::make(limits);
    if (created.is_err() && fatal.is_none())
        fatal = Some(http2_error(rstd::move(created).unwrap_err()));
    Option<lihttpto::h2::ClientConnection> protocol;
    if (created.is_ok()) protocol = Some(rstd::move(created).unwrap());
    TunnelIo                    io;
    Vec<TunnelWire>             outgoing;
    Option<TunnelWire>          current;
    Vec<Arc<TunnelStreamState>> streams;
    u64                         scheduled {}, completed {};
    auto settings_deadline = Instant::now() + pool->native->options.connect_timeout;
    auto idle_deadline     = Instant::now() + pool->native->options.idle_timeout;
    auto find              = [&](u32 id) -> TunnelStreamState* {
        for (const auto& stream : streams)
            if (stream->id == id) return stream.as_ptr().as_raw_ptr();
        return nullptr;
    };
    auto queue = [&] {
        if (protocol.is_none()) return;
        auto output = protocol->take_output();
        for (auto& item : output) {
            ++scheduled;
            if (item.stream != u32())
                if (auto* stream = find(item.stream)) stream->last_serial = scheduled;
            outgoing.push(TunnelWire { rstd::move(item), scheduled });
        }
    };
    while (fatal.is_none()) {
        auto event    = owner->changed.listen();
        bool progress = false, closing;
        auto now      = Instant::now();
        {
            auto f  = owner->fields.lock().unwrap();
            closing = f->closing;
            for (const auto& stream : f->streams) {
                bool known = false;
                for (const auto& existing : streams)
                    if (Arc<TunnelStreamState>::ptr_eq(stream, existing)) known = true;
                if (! known) {
                    streams.push(stream.clone());
                    progress = true;
                }
            }
        }
        if (closing) break;
        if (! protocol->established() && now >= settings_deadline) {
            fatal = Some(connect_failure(
                Error::Timeout(), ConnectProgress { ConnectStage::Http2Settings, owner->id }));
            break;
        }
        if (io.written.is_some()) {
            progress    = true;
            auto result = io.written.take().unwrap();
            if (result.is_err()) {
                fatal = Some(rstd::move(result).unwrap_err());
                break;
            }
            if (*result == usize() || *result > current->output.bytes.len() - current->offset) {
                fatal = Some(Error::Contract("invalid tunnel write completion"_Str));
                break;
            }
            auto& wire  = *current;
            auto  begin = rstd::cmp::max(wire.offset, wire.output.payload_offset);
            auto  end   = rstd::cmp::min(wire.offset + *result,
                                         wire.output.payload_offset + wire.output.payload_length);
            if (end > begin)
                if (auto* stream = find(wire.output.stream)) {
                    auto f = stream->fields.lock().unwrap();
                    if (f->write.is_some()) {
                        f->write->confirmed += end - begin;
                        f->write->done = f->write->confirmed == f->write->bytes.len();
                    }
                    stream->changed.notify();
                }
            wire.offset += *result;
            if (wire.offset == wire.output.bytes.len()) {
                completed = wire.serial;
                current   = None();
            }
        }
        if (io.received.is_some()) {
            progress    = true;
            auto result = io.received.take().unwrap();
            if (result.is_err()) {
                fatal = Some(rstd::move(result).unwrap_err());
                break;
            }
            if (result->is_empty()) {
                fatal = Some(protocol_error("EOF in HTTP/2 tunnel connection"_Str));
                break;
            }
            auto fed = protocol->feed(rstd::move(*result));
            if (fed.is_err()) {
                fatal = Some(http2_error(rstd::move(fed).unwrap_err()));
                break;
            }
        }
        if (protocol->established() &&
            owner->connecting->connect_stage() == ConnectStage::Http2Settings)
            owner->connecting->set_connect_stage(ConnectStage::ConnectHead);
        auto events = protocol->take_events();
        for (auto& item : events) {
            progress = true;
            RSTD_MATCH(item) {
                RSTD_CASE(Head, stream, head, informational) {
                    auto* s = find(stream);
                    if (! s || informational) break;
                    auto code = head.status.value();
                    if (code < u16(200) || code >= u16(300)) {
                        ProxyConnectFailure failure { rstd::move(head) };
                        s->fail(Error::Extension(
                            "http CONNECT"_Str,
                            Box<rstd::dyn<rstd::error::Error>>::make(rstd::move(failure))));
                    } else {
                        {
                            auto f   = s->fields.lock().unwrap();
                            f->head  = Some(rstd::move(head));
                            f->stage = Some(ConnectStage::TunnelIo);
                        }
                        s->changed.notify();
                    }
                    break;
                }
                RSTD_CASE(Data, stream, bytes) {
                    auto* s = find(stream);
                    if (! s) break;
                    {
                        auto f = s->fields.lock().unwrap();
                        f->buffered += bytes.len();
                        f->incoming.push(rstd::move(bytes));
                    }
                    s->changed.notify();
                    break;
                }
                RSTD_CASE(Trailers, stream, headers) {
                    auto* s = find(stream);
                    if (s) s->fail(protocol_error("trailers in CONNECT tunnel"_Str));
                    break;
                }
                RSTD_CASE(End, stream) {
                    auto* s = find(stream);
                    if (! s) break;
                    {
                        auto f        = s->fields.lock().unwrap();
                        f->remote_end = true;
                    }
                    s->changed.notify();
                    break;
                }
                RSTD_CASE(Reset, stream, code) {
                    auto* s = find(stream);
                    if (! s) break;
                    bool graceful;
                    {
                        auto f   = s->fields.lock().unwrap();
                        graceful = code == u32() && f->remote_end &&
                                   (f->head.is_some() || f->transferred) && f->error.is_none() &&
                                   ! f->stopping;
                        if (graceful) {
                            f->graceful_reset = true;
                            f->local_end      = true;
                        }
                    }
                    if (graceful) {
                        s->reset         = true;
                        s->stop_deadline = Some(now + pool->native->options.tls_close_timeout);
                        s->changed.notify();
                        break;
                    }
                    s->fail(http2_error(lihttpto::h2::Error::Stream(stream, code)));
                    break;
                }
                RSTD_CASE(Goaway, last_stream, code) {
                    {
                        auto f      = owner->fields.lock().unwrap();
                        f->draining = true;
                    }
                    for (const auto& s : streams)
                        if (s->id == u32() || s->id > last_stream)
                            s->fail(http2_error(lihttpto::h2::Error::Stream(s->id, u32(7))));
                    if (code != u32())
                        fatal = Some(http2_error(lihttpto::h2::Error::Connection(code)));
                    break;
                }
            }
        }
        Option<Instant> wake_at;
        auto            wake = [&](Instant time) {
            if (wake_at.is_none() || time < *wake_at) wake_at = Some(time);
        };
        if (! protocol->established()) wake(settings_deadline);
        for (const auto& s : streams) {
            bool stopping;
            {
                auto f = s->fields.lock().unwrap();
                if (f->head.is_none() && ! f->transferred && f->error.is_none()) {
                    if (now >= s->deadline) {
                        auto stage  = f->stage.unwrap_or(owner->connecting->connect_stage());
                        f->error    = Some(connect_failure(
                            Error::Timeout(), ConnectProgress { stage, owner->id, s->id }));
                        f->stage    = Some(stage);
                        f->failed   = true;
                        f->stopping = true;
                    } else
                        wake(s->deadline);
                }
                stopping = f->stopping;
                if (f->consumed != usize()) {
                    protocol->consume(s->id, f->consumed);
                    f->consumed = usize();
                    progress    = true;
                }
            }
            if (! stopping && s->id == u32() && ! protocol->accepting()) {
                s->fail(http2_error(lihttpto::h2::Error::Stream(u32(), u32(7))));
                stopping = true;
            }
            if (stopping && ! s->reset) {
                if (s->id != u32()) protocol->reset(s->id);
                s->reset         = true;
                s->stop_deadline = Some(now + pool->native->options.tls_close_timeout);
                progress         = true;
            }
            if (! stopping && s->id == u32() && protocol->accepting()) {
                auto opened = protocol->open(s->request);
                if (opened.is_ok()) {
                    s->id = *opened;
                    {
                        auto f       = s->fields.lock().unwrap();
                        f->stream_id = s->id;
                    }
                    s->request = {};
                    progress   = true;
                }
            }
            if (! stopping && s->id != u32()) {
                auto f = s->fields.lock().unwrap();
                if (f->head.is_some() || f->transferred) {
                    if (f->write.is_some() && ! f->write->done && ! f->local_end) {
                        auto& write = *f->write;
                        auto  count =
                            rstd::min(write.bytes.len() - write.queued, protocol->writable(s->id));
                        if (count != usize()) {
                            auto sent = protocol->send(
                                s->id, write.bytes.slice(write.queued, write.queued + count));
                            if (sent.is_err()) {
                                f->error    = Some(connect_failure(
                                    http2_error(rstd::move(sent).unwrap_err()),
                                    ConnectProgress { ConnectStage::TunnelIo, owner->id, s->id }));
                                f->stopping = true;
                                f->failed   = true;
                            } else {
                                write.queued += count;
                                progress = true;
                            }
                        }
                    }
                    if (f->shutdown_requested && ! f->local_end && f->write.is_none()) {
                        auto sent = protocol->send(s->id, Bytes {}, true);
                        if (sent.is_err()) {
                            f->error    = Some(connect_failure(
                                http2_error(rstd::move(sent).unwrap_err()),
                                ConnectProgress { ConnectStage::TunnelIo, owner->id, s->id }));
                            f->stopping = true;
                            f->failed   = true;
                        } else {
                            f->local_end = true;
                            progress     = true;
                        }
                    }
                }
            }
            if (s->stop_deadline.is_some()) {
                if (now >= *s->stop_deadline && completed < s->last_serial) {
                    fatal = Some(Error::Timeout());
                    break;
                }
                wake(*s->stop_deadline);
            }
        }
        queue();
        for (usize i; i < streams.len();) {
            auto s = streams[i].clone();
            bool done;
            {
                auto f = s->fields.lock().unwrap();
                if (f->local_end && completed >= s->last_serial) {
                    f->shutdown_done = true;
                    s->changed.notify();
                }
                done = (s->reset || (f->local_end && f->remote_end && f->buffered == usize())) &&
                       completed >= s->last_serial;
                if (done) {
                    f->released = true;
                    if (! f->graceful_reset || f->stopping) {
                        f->incoming = {};
                        f->buffered = usize();
                    }
                    if (f->write.is_some()) f->write->bytes = {};
                    if (f->buffered == usize()) f->permit = None();
                }
            }
            if (! done) {
                ++i;
                continue;
            }
            if (s->id != u32()) protocol->forget(s->id);
            s->changed.notify();
            streams.remove(i);
            progress = true;
            {
                auto f = owner->fields.lock().unwrap();
                for (usize j; j < f->streams.len(); ++j)
                    if (Arc<TunnelStreamState>::ptr_eq(s, f->streams[j])) {
                        f->streams.remove(j);
                        break;
                    }
            }
        }
        if (fatal.is_some()) break;
        bool draining;
        {
            auto f        = owner->fields.lock().unwrap();
            f->peer_limit = protocol->capacity();
            draining      = f->draining;
        }
        if (streams.is_empty()) {
            if (draining || now >= idle_deadline) break;
            wake(idle_deadline);
        } else
            idle_deadline = now + pool->native->options.idle_timeout;
        if (current.is_none() && ! outgoing.is_empty()) current = Some(outgoing.remove(usize()));
        if (io.write.is_none() && current.is_some())
            io.write = Some(native
                                ->send_confirmed(current->output.bytes.slice(
                                    current->offset, current->output.bytes.len()))
                                .into_future());
        if (io.read.is_none()) io.read = Some(native->recv(usize(16384)).into_future());
        if (progress) continue;
        co_await TunnelWake { &io,
                              rstd::move(event),
                              wake_at.is_some() ? Some(rstd::async::into_future(
                                                      rstd::async::sleep(remaining(*wake_at))))
                                                : None<TunnelTimer>() };
    }
    {
        auto f     = owner->fields.lock().unwrap();
        f->closing = true;
    }
    io.read  = None();
    io.write = None();
    outgoing.clear();
    current = None();
    Option<Error> cleanup;
    if (native.is_some()) {
        auto closed = co_await native->close();
        if (closed.is_err()) cleanup = Some(rstd::move(closed).unwrap_err());
    }
    {
        auto f = owner->fields.lock().unwrap();
        for (const auto& s : f->streams) {
            s->fail(Error::Closed());
            {
                auto b      = s->fields.lock().unwrap();
                b->released = true;
                b->stopping = true;
                b->incoming = {};
                b->buffered = usize();
                if (b->write.is_some()) b->write->bytes = {};
                b->permit = None();
            }
            s->notify();
        }
        f->streams.clear();
        f->done = true;
        if (cleanup.is_some()) f->errors.push(cleanup.take().unwrap());
        if (fatal.is_some()) f->errors.push(fatal.take().unwrap());
        auto connecting = owner->connecting->fields.lock().unwrap();
        for (auto& error : connecting->cleanup_errors) f->errors.push(rstd::move(error));
        connecting->cleanup_errors.clear();
    }
    owner->permit = None();
    owner->changed.notify();
    co_return empty {};
}
PendingTunnel::PendingTunnel(Arc<TunnelStreamState> state, Arc<TunnelPool> pool)
    : state_(rstd::move(state)), pool_(rstd::move(pool)) {
}
PendingTunnel::PendingTunnel(PendingTunnel&&) noexcept = default;
auto PendingTunnel::operator=(PendingTunnel&& other) noexcept -> PendingTunnel& {
    if (this != &other) {
        abandon();
        state_ = rstd::move(other.state_);
        pool_  = rstd::move(other.pool_);
    }
    return *this;
}
PendingTunnel::~PendingTunnel() {
    abandon();
}
void PendingTunnel::abandon() const {
    if (! state_) return;
    bool transferred;
    {
        auto f      = state_->fields.lock().unwrap();
        transferred = f->transferred;
    }
    if (! transferred) state_->stop();
}
void PendingTunnel::request_stop() const {
    if (state_) state_->stop();
}
auto PendingTunnel::close() const -> coro<Result<empty>> {
    return tunnel_close(state_.clone());
}
auto tunnel_connected(Arc<TunnelPool> pool, Arc<TunnelStreamState> state)
    -> coro<Result<TunnelConnection>> {
    {
        auto f = state->fields.lock().unwrap();
        if (f->claimed) co_return Err(Error::Busy());
        f->claimed = true;
    }
    TunnelOperation operation { state.clone(), true };
    for (;;) {
        auto event = state->changed.listen();
        {
            auto f = state->fields.lock().unwrap();
            if (f->error.is_some()) {
                operation.armed = false;
                co_return Err(f->error.take().unwrap());
            }
            if (f->stopping) {
                operation.armed = false;
                co_return Err(Error::Canceled());
            }
            if (f->head.is_some()) {
                auto head       = f->head.take().unwrap();
                f->transferred  = true;
                operation.armed = false;
                auto backend    = Arc<rstd::dyn<rstd::async::io::CompletionStream>>::make(
                    TunnelStream { pool.clone(), state.clone() });
                co_return Ok(TunnelConnection(rstd::move(head),
                                              rstd::async::io::Stream(rstd::move(backend))));
            }
        }
        co_await rstd::move(event);
    }
}
auto PendingTunnel::connected() const -> coro<Result<TunnelConnection>> {
    return tunnel_connected(pool_.clone(), state_.clone());
}
auto PendingTunnel::progress() const -> ConnectProgress {
    return state_->progress();
}
TunnelClient::TunnelClient(Arc<TunnelPool> pool, Arc<ResourceLease<empty>> registration)
    : pool_(rstd::move(pool)), registration_(rstd::move(registration)) {
}
TunnelClient::TunnelClient(TunnelClient&&) noexcept = default;
auto TunnelClient::operator=(TunnelClient&& other) noexcept -> TunnelClient& {
    if (this != &other) {
        abandon();
        pool_         = rstd::move(other.pool_);
        registration_ = rstd::move(other.registration_);
    }
    return *this;
}
TunnelClient::~TunnelClient() {
    abandon();
}
void TunnelClient::abandon() const {
    if (! pool_) return;
    bool last;
    {
        auto f = pool_->fields.lock().unwrap();
        last   = --f->handles == usize();
    }
    if (last) registration_->request_stop();
}
auto TunnelClient::clone() const -> TunnelClient {
    {
        auto f = pool_->fields.lock().unwrap();
        ++f->handles;
    }
    return TunnelClient(pool_.clone(), registration_.clone());
}
void TunnelClient::request_close() const {
    registration_->request_stop();
}
auto TunnelClient::close() const -> coro<Result<empty>> {
    return registration_->release();
}
auto TunnelClient::wait_closed() const -> coro<CleanupReport> {
    request_close();
    return registration_->wait_released();
}
auto TunnelClient::take_close_result() const -> Option<Result<empty>> {
    return registration_->take_release_result();
}
auto TunnelClient::make(ResourceContext  resources,
                        lihttpto::Url    proxy_url,
                        RouteChoice      route,
                        TransportOptions options) -> Result<TunnelClient> {
#if LITO_FEAT_HTTP2 && LITO_FEAT_TLS
    auto origin = proxy_url.http_origin();
    if (origin.is_none() || origin->scheme != "https"_str || proxy_url.userinfo().is_some() ||
        proxy_url.query().is_some() || proxy_url.fragment().is_some() ||
        (! proxy_url.path().is_empty() && proxy_url.path() != "/"_str) || options.proxy.enabled())
        return Err(
            Error::InvalidOptions("CONNECT client requires a direct HTTPS proxy origin"_Str));
    if (route.route.is_some() && (*route.route)->origin() != origin->serialize().as_str())
        return Err(Error::InvalidOptions("CONNECT route origin mismatch"_Str));
    if (options.tls.is_none()) {
        auto tls = TlsProfile::make({}, VersionPolicy::Http2);
        if (tls.is_err()) return Err(rstd::move(tls).unwrap_err());
        options.tls = Some(rstd::move(tls).unwrap());
    }
    if (options.tls->version_policy() == VersionPolicy::Http1)
        return Err(Error::InvalidOptions("CONNECT client requires h2 ALPN"_Str));
    auto backend = NativeBackend::make(resources.clone(), rstd::move(options));
    if (backend.is_err()) return Err(rstd::move(backend).unwrap_err());
    auto native = backend->state_.clone();
    auto pool   = Arc<TunnelPool>::make(rstd::move(*backend),
                                        rstd::move(native),
                                        resources.clone(),
                                        rstd::move(proxy_url),
                                        rstd::move(route));
    auto lease  = resources.attach(
        empty {}, Arc<rstd::dyn<ManagedResource>>::make(TunnelPoolOwner { pool.clone() }));
    if (lease.is_err()) return Err(rstd::move(lease).unwrap_err().error);
    return Ok(TunnelClient(rstd::move(pool), Arc<ResourceLease<empty>>::make(rstd::move(*lease))));
#else
    (void)resources;
    (void)proxy_url;
    (void)route;
    (void)options;
    return Err(Error::Unsupported("CONNECT tunnels require features tls and http2"_Str));
#endif
}
auto TunnelClient::open(String authority, Vec<lihttpto::h2::Field> fields) const
    -> Result<PendingTunnel> {
    auto request = lihttpto::h2::connect_request(
        authority.as_str(), fields.as_slice(), pool_->native->options.head_limit);
    if (request.is_err())
        return Err(Error::Protocol(
            "invalid CONNECT request"_Str,
            Some(Box<rstd::dyn<rstd::error::Error>>::make(rstd::move(request).unwrap_err()))));
    auto& options = pool_->native->options;
    auto  permit  = pool_->resources.try_reserve(ResourceAmount {
        .bytes = options.receive_window + options.upload_window + options.head_limit });
    if (permit.is_err()) return Err(rstd::move(permit).unwrap_err());
    auto f = pool_->fields.lock().unwrap();
    if (f->closed || pool_->resources.closed()) return Err(Error::Closed());
    Option<Arc<TunnelSession>> choice;
    usize                      active;
    for (usize i; i < f->sessions.len();) {
        auto session = f->sessions[i].clone();
        auto s       = session->fields.lock().unwrap();
        if (s->done && session->task.is_some() && session->task->is_finished()) {
            for (auto& error : s->errors) f->errors.push(rstd::move(error));
            s->errors.clear();
            f->sessions.remove(i);
            continue;
        }
        if (! s->done) ++active;
        if (! s->closing && ! s->draining && s->streams.len() < s->peer_limit && choice.is_none())
            choice = Some(session.clone());
        ++i;
    }
    if (choice.is_none()) {
        if (active >= rstd::min(pool_->native->options.max_connections,
                                pool_->native->options.max_connections_per_origin))
            return Err(Error::Busy());
        auto permit = pool_->resources.try_reserve(ResourceAmount { .resources = usize(1) });
        if (permit.is_err()) return Err(rstd::move(permit).unwrap_err());
        if (f->next_session_id == u64::MAX) return Err(Error::Capacity());
        auto session = Arc<TunnelSession>::make(
            f->next_session_id++, pool_->native->options.max_http2_streams, rstd::move(*permit));
        f->sessions.push(session.clone());
        choice = Some(rstd::move(session));
    }

    auto selected = choice.take().unwrap();
    auto state    = Arc<TunnelStreamState>::make(rstd::move(*request),
                                                 rstd::move(*permit),
                                                 selected->changed.clone(),
                                                 options,
                                                 selected->id,
                                                 selected->connecting.clone());
    {
        auto s = selected->fields.lock().unwrap();
        s->streams.push(state.clone());
    }
    if (selected->task.is_none()) {
        auto task = pool_->resources.spawn(TunnelTask { pool_.downgrade(), selected.downgrade() });
        if (task.is_err()) {
            state->fail(rstd::move(task).unwrap_err().error);
            {
                auto s  = selected->fields.lock().unwrap();
                s->done = s->closing = true;
                s->streams.clear();
            }
            {
                auto s      = state->fields.lock().unwrap();
                s->released = true;
                s->permit   = None();
            }
            selected->permit = None();
            return Err(state->fields.lock().unwrap()->error.take().unwrap());
        }
        selected->task = Some(rstd::move(*task));
    }
    selected->changed.notify();
    return Ok(PendingTunnel(rstd::move(state), pool_.clone()));
}
} // namespace lifetchto::http
