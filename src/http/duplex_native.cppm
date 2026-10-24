module;
#include <rstd/macro.hpp>
module lifetchto.http:duplex_native;
import :connection;
using namespace rstd::prelude;
using namespace rstd::literals;
namespace lifetchto::http
{
struct NativeDuplexFields {
    Option<DuplexChannel> channel;
    Option<Connection>    connection;
    Option<TaskControl>   connecting, reader, writer;
    Option<Result<empty>> ready;
    bool                  launched {};
};
struct NativeDuplexState {
    Arc<BodyState>                        body { Arc<BodyState>::make(None<Instant>()) };
    rstd::sync::Mutex<NativeDuplexFields> fields { NativeDuplexFields {} };
    rstd::sync::Weak<NativeState>         backend;
    Arc<rstd::sync::atomic::Atomic<bool>> native_done { Arc<rstd::sync::atomic::Atomic<bool>>::make(
        false) };
    Event                                 changed;
    Option<ResourcePermit>                permit;
    explicit NativeDuplexState(rstd::sync::Weak<NativeState> backend)
        : backend(rstd::move(backend)) {}
    void stop(StopReason reason) const {
        body->cancel(reason);
        auto f = fields.lock().unwrap();
        if (f->channel.is_some()) f->channel->request_stop(reason);
        changed.notify();
    }
    void ready(Result<empty> value) const {
        auto f = fields.lock().unwrap();
        if (value.is_err() && f->channel.is_some())
            f->channel->fail(value.as_ref().unwrap_err().clone());
        f->ready = Some(rstd::move(value));
        changed.notify();
    }
};
struct NativeDuplexReader {
    Arc<NativeDuplexState> state;
    DuplexChannel          channel;
    usize                  window;
    void                   request_stop(StopReason reason) const { state->stop(reason); }
    auto                   run(Cancellation) const -> coro<Result<empty>> {
        auto stop = channel.cancellation();
        for (;;) {
            auto reading = [&] {
                auto f = state->fields.lock().unwrap();
                return f->connection->recv(window);
            }();
            auto result = co_await rstd::async::select(rstd::move(reading).into_future(),
                                                       stop.stopped().into_future());
            if (result.is_right()) co_return Ok(empty {});
            auto bytes = rstd::move(result).unwrap_left();
            if (bytes.is_err()) {
                channel.fail(rstd::move(bytes).unwrap_err());
                co_return Ok(empty {});
            }
            if (bytes->is_empty()) {
                channel.end_input();
                co_return Ok(empty {});
            }
            auto queued = co_await channel.receive(rstd::move(bytes).unwrap());
            if (queued.is_err()) {
                channel.fail(rstd::move(queued).unwrap_err());
                co_return Ok(empty {});
            }
        }
    }
};
auto native_half_close(Connection& connection) -> Result<usize> {
#if LITO_FEAT_TLS
    if (connection.tls.is_some())
        return Err(Error::Unsupported("TLS send half-close is unavailable"_Str));
#endif
    auto result = [&] {
#if RSTD_OS_UNIX
        if (connection.unix.is_some()) return connection.unix->shutdown();
#endif
        return connection.tcp->shutdown();
    }();
    if (result.is_err()) return Err(Error::Connect(result.unwrap_err()));
    return Ok(usize());
}
struct NativeDuplexWriter {
    Arc<NativeDuplexState> state;
    DuplexChannel          channel;
    void                   request_stop(StopReason reason) const { state->stop(reason); }
    auto                   run(Cancellation) const -> coro<Result<empty>> {
        auto stop = channel.cancellation();
        for (;;) {
            auto next = co_await channel.next_write();
            if (next.is_err()) co_return Ok(empty {});
            auto command = rstd::move(next).unwrap();
            if (command.is_half_close()) {
                auto f = state->fields.lock().unwrap();
                command.complete(native_half_close(*f->connection));
                continue;
            }
            auto writing = [&] {
                auto f = state->fields.lock().unwrap();
                return f->connection->send(command.bytes().clone());
            }();
            auto result = co_await rstd::async::select(rstd::move(writing).into_future(),
                                                       stop.stopped().into_future());
            if (result.is_right()) {
                command.complete(Err(stop_error(rstd::move(result).unwrap_right())));
                co_return Ok(empty {});
            }
            command.complete(rstd::move(result).unwrap_left());
        }
    }
};
struct NativeDuplexConnect {
    Arc<NativeState>       backend;
    Arc<NativeDuplexState> state;
    DuplexRequest          request;
    ResourceContext        resources;
    Option<ProxyEndpoint>  proxy;
    void                   request_stop(StopReason reason) const { state->stop(reason); }
    auto                   run(Cancellation) const -> coro<Result<empty>> {
        auto resources = this->resources.clone();
        auto slot = Arc<Slot>::make(request.url.http_origin()->serialize(),
                                    request.route.route.clone(),
                                    proxy.is_some() ? Some(proxy->clone()) : None<ProxyEndpoint>());
        auto connection = co_await connect_to(
            backend.clone(), slot.clone(), request.url, state->body.clone(), true);
        if (connection.is_err()) {
            state->ready(Err(rstd::move(connection).unwrap_err()));
            co_return Ok(empty {});
        }
        if (connection->http2) {
            (void)co_await connection->close();
            state->ready(Err(Error::Unsupported("duplex requires an HTTP/1 TLS profile"_Str)));
            co_return Ok(empty {});
        }
        auto channel = [&] {
            auto f        = state->fields.lock().unwrap();
            f->connection = Some(rstd::move(connection).unwrap());
            return f->channel->clone();
        }();
        auto reader = resources.spawn(NativeDuplexReader {
            state.clone(),
            channel.clone(),
            rstd::min(backend->options.receive_window, backend->options.upload_window) });
        if (reader.is_err()) {
            state->ready(Err(rstd::move(reader).unwrap_err().error));
            co_return Ok(empty {});
        }
        state->fields.lock().unwrap()->reader = Some(rstd::move(reader).unwrap());
        auto writer = resources.spawn(NativeDuplexWriter { state.clone(), channel.clone() });
        if (writer.is_err()) {
            state->ready(Err(rstd::move(writer).unwrap_err().error));
            co_return Ok(empty {});
        }
        state->fields.lock().unwrap()->writer = Some(rstd::move(writer).unwrap());
        state->ready(Ok(empty {}));
        co_return Ok(empty {});
    }
};
struct NativeDuplexOwner {
    Arc<NativeDuplexState> state;
    void                   request_stop(StopReason reason) const { state->stop(reason); }
    auto                   release(ReleaseContext) const -> coro<Result<empty>> {
        state->stop(StopReason::OwnerClosed);
        for (;;) {
            auto event = state->changed.listen();
            if (state->fields.lock().unwrap()->launched) break;
            co_await rstd::move(event);
        }
        Option<TaskControl> connecting;
        {
            auto f = state->fields.lock().unwrap();
            if (f->connecting.is_some()) connecting = Some(f->connecting->clone());
        }
        if (connecting.is_some()) (void)co_await connecting->wait();
        Option<TaskControl> reader, writer;
        {
            auto f = state->fields.lock().unwrap();
            reader = f->reader.take();
            writer = f->writer.take();
        }
        if (reader.is_some()) (void)co_await reader->wait();
        if (writer.is_some()) (void)co_await writer->wait();
        Option<Connection>    connection;
        Option<DuplexChannel> channel;
        {
            auto f     = state->fields.lock().unwrap();
            connection = f->connection.take();
            if (f->channel.is_some()) channel = Some(f->channel->clone());
        }
        Result<empty> closed = Ok(empty {});
        if (connection.is_some()) {
            closed = co_await connection->close(ConnectionClose::SendOnly);
        }
        if (channel.is_some()) channel->release_buffers();
        state->permit = None();
        state->native_done->store(true);
        auto backend = state->backend.upgrade();
        if (backend) backend->changed.notify();
        co_return rstd::move(closed);
    }
};
struct NativeDuplexOperation {
    Arc<NativeState>                                     backend;
    DuplexRequest                                        request;
    Arc<NativeDuplexState>                               state;
    rstd::sync::Mutex<Option<Arc<ResourceLease<empty>>>> registration {
        None<Arc<ResourceLease<empty>>>()
    };
    Option<ProxyEndpoint> proxy;
    NativeDuplexOperation(Arc<NativeState>      backend,
                          DuplexRequest         request,
                          Option<ProxyEndpoint> proxy)
        : backend(rstd::move(backend)),
          request(rstd::move(request)),
          state(Arc<NativeDuplexState>::make(this->backend.downgrade())),
          proxy(rstd::move(proxy)) {}
};
struct DuplexLaunch {
    Arc<NativeDuplexState> state;
    ~DuplexLaunch() {
        state->fields.lock().unwrap()->launched = true;
        state->changed.notify();
    }
};
struct NativeDuplexDriver {
    Arc<NativeDuplexOperation> operation;
    void request_stop(StopReason reason) const { operation->state->stop(reason); }
    auto run(TransferContext context, OutputSender<DuplexStream> output) const
        -> coro<ProtocolCompletion>;
    auto release(ReleaseContext) const -> coro<Result<empty>> {
        auto lease = [&] {
            auto value = operation->registration.lock().unwrap();
            return value->clone();
        }();
        if (lease.is_some()) co_return co_await (*lease)->release();
        auto state = operation->state.clone();
        {
            auto f = state->fields.lock().unwrap();
            if (f->channel.is_some()) f->channel->release_buffers();
        }
        state->permit = None();
        state->native_done->store(true);
        co_return Ok(empty {});
    }
};
} // namespace lifetchto::http
namespace rstd
{
template<>
struct Impl<lifetchto::ManagedResource, lifetchto::http::NativeDuplexOwner>
    : ImplBase<lifetchto::http::NativeDuplexOwner> {
    void request_stop(lifetchto::StopReason reason) const { this->self().request_stop(reason); }
    auto release(lifetchto::ReleaseContext context) const { return this->self().release(context); }
};
} // namespace rstd
namespace lifetchto::http
{
auto NativeDuplexDriver::run(TransferContext context, OutputSender<DuplexStream> output) const
    -> coro<ProtocolCompletion> {
    auto state            = operation->state.clone();
    state->body->deadline = context.deadline();
    auto attempt          = context.begin_attempt();
    if (attempt.is_err())
        co_return ProtocolCompletion { Err(rstd::move(attempt).unwrap_err()),
                                       RemoteOutcome::NotDispatched };
    auto permit = co_await context.resources().reserve(ResourceAmount {
        .resources = usize(1), .bytes = operation->backend->options.receive_window });
    if (permit.is_err())
        co_return ProtocolCompletion { Err(rstd::move(permit).unwrap_err()),
                                       RemoteOutcome::NotDispatched };
    state->permit = Some(rstd::move(permit).unwrap());
    auto channel =
        co_await DuplexChannel::make(context.clone(),
                                     rstd::min(operation->backend->options.receive_window,
                                               operation->backend->options.upload_window),
                                     operation->request.url.scheme() != Some("https"_str));
    if (channel.is_err())
        co_return ProtocolCompletion { Err(rstd::move(channel).unwrap_err()),
                                       RemoteOutcome::NotDispatched };
    state->fields.lock().unwrap()->channel = Some(channel->clone());
    auto attached                          = context.resources().attach(
        empty {}, Arc<rstd::dyn<ManagedResource>>::make(NativeDuplexOwner { state.clone() }));
    if (attached.is_err())
        co_return ProtocolCompletion { Err(rstd::move(attached).unwrap_err().error),
                                       RemoteOutcome::NotDispatched };
    auto lease = Arc<ResourceLease<empty>>::make(rstd::move(attached).unwrap());
    *operation->registration.lock().unwrap() = Some(lease.clone());
    {
        DuplexLaunch launch { state.clone() };
        auto         f = operation->backend->fields.lock().unwrap();
        if (f->closed)
            co_return ProtocolCompletion { Err(Error::Closed()), RemoteOutcome::NotDispatched };
        if (f->slots.len() + f->retired.len() + active_duplex(*f) >=
            operation->backend->options.max_connections)
            co_return ProtocolCompletion { Err(Error::Capacity()), RemoteOutcome::NotDispatched };
        f->duplex.push({ lease.downgrade(), state->native_done.clone() });
        auto connecting = context.resources().spawn(NativeDuplexConnect {
            operation->backend.clone(),
            state.clone(),
            operation->request.clone(),
            context.resources(),
            operation->proxy.is_some() ? Some(operation->proxy->clone()) : None<ProxyEndpoint>() });
        if (connecting.is_err())
            co_return ProtocolCompletion { Err(rstd::move(connecting).unwrap_err().error),
                                           RemoteOutcome::NotDispatched };
        state->fields.lock().unwrap()->connecting = Some(rstd::move(connecting).unwrap());
    }
    for (;;) {
        auto event = state->changed.listen();
        {
            auto f = state->fields.lock().unwrap();
            if (f->ready.is_some()) {
                if (f->ready->is_err())
                    co_return ProtocolCompletion { Err(f->ready->as_ref().unwrap_err().clone()),
                                                   RemoteOutcome::Unknown };
                break;
            }
        }
        co_await rstd::move(event);
    }
    auto sent = output.publish(channel->take_stream().unwrap());
    if (sent.is_err()) channel->request_stop();
    co_return ProtocolCompletion { co_await channel->wait_finished(), RemoteOutcome::Unknown };
}
auto NativeBackend::duplex(DuplexRequest input) const
    -> rstd::Result<PreparedOperation<DuplexStream>, Rejected<DuplexRequest>> {
    Option<ProxyEndpoint> proxy;
    auto                  valid = [&]() -> Result<empty> {
        if (state_->fields.lock().unwrap()->closed) return Err(Error::Closed());
        if (input.route.route.is_some() && state_->options.proxy.enabled())
            return Err(Error::Unsupported("explicit routes require a disabled proxy profile"_Str));
        auto origin = input.url.http_origin();
        if (origin.is_none() || input.url.userinfo().is_some())
            return Err(Error::InvalidOptions("HTTP origin required for duplex"_Str));
        if (input.route.route.is_some() &&
            (*input.route.route)->origin() != origin->serialize().as_str())
            return Err(Error::InvalidOptions("duplex route belongs to another origin"_Str));
        if (input.route.route.is_some() && (*input.route.route)->unix_path().is_some() &&
            state_->options.tcp.specified())
            return Err(Error::Unsupported("TCP options cannot be applied to Unix sockets"_Str));
#if ! LITO_FEAT_TLS
        if (origin->scheme.as_str() == "https"_str)
            return Err(Error::Unsupported("TLS is disabled"_Str));
#else
        if (origin->scheme.as_str() == "https"_str &&
            state_->options.tls->version_policy() == VersionPolicy::Http2)
            return Err(Error::Unsupported("duplex requires an HTTP/1 TLS profile"_Str));
#endif
        auto selected = state_->options.proxy.select(input.url);
        if (selected.is_err()) return Err(rstd::move(selected).unwrap_err());
        if (selected->is_some() && input.route.route.is_some())
            return Err(Error::InvalidOptions("proxy conflicts with an explicit route"_Str));
        proxy = rstd::move(selected).unwrap();
        return Ok(empty {});
    }();
    if (valid.is_err())
        return Err(Rejected<DuplexRequest> { rstd::move(valid).unwrap_err(), rstd::move(input) });
    return Ok(PreparedOperation<DuplexStream>::make(NativeDuplexDriver {
        Arc<NativeDuplexOperation>::make(state_.clone(), rstd::move(input), rstd::move(proxy)) }));
}
} // namespace lifetchto::http
