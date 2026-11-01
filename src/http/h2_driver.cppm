module;
#include <rstd/enum.hpp>
module lifetchto.http:h2_driver;
import :connection;
import :state;
using namespace rstd::prelude;
using namespace rstd::literals;
namespace lifetchto::http
{
using H2Read  = coro<Result<Bytes>>::Future;
using H2Write = coro<Result<usize>>::Future;
using H2Pull  = coro<Result<Option<Bytes>>>::Future;
using H2Close = coro<Result<empty>>::Future;
using H2Timer = decltype(rstd::async::into_future(rstd::async::sleep(Duration {})));
struct H2Stream {
    Work                          work;
    u32                           id {};
    Option<H2Pull>                pulling;
    Option<Result<Option<Bytes>>> pulled;
    Option<H2Close>               closing;
    Option<Result<empty>>         closed;
    u64                           accepted {};
    Bytes                         pending;
    usize                         buffered {};
    u64                           last_serial {};
    bool                          local_done {}, remote_done {}, stopped {}, early {};
    Instant                       continue_at;
    H2Stream(Work work, Duration expect)
        : work(rstd::move(work)), continue_at(Instant::now() + expect) {}
};
struct H2Io {
    Option<H2Read>        read;
    Option<H2Write>       write;
    Option<Result<Bytes>> received;
    Option<Result<usize>> written;
    u64                   writing_serial {}, scheduled {}, completed {};
};
struct H2Wake {
    H2Io*               io;
    Vec<Arc<H2Stream>>* streams;
    EventWait           changed;
    Option<H2Timer>     timer;
    using Output = empty;
    auto poll(mut_ref<H2Wake> self, rstd::task::Context& cx) -> rstd::task::Poll<empty> {
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
        for (const auto& stream : *self->streams) {
            if (stream->pulling.is_some()) {
                auto next = rstd::future::poll(*stream->pulling, cx);
                if (next.is_ready()) {
                    stream->pulled  = Some(rstd::move(next).take());
                    stream->pulling = None();
                    ready           = true;
                }
            }
            if (stream->closing.is_some()) {
                auto next = rstd::future::poll(*stream->closing, cx);
                if (next.is_ready()) {
                    stream->closed  = Some(rstd::move(next).take());
                    stream->closing = None();
                    ready           = true;
                }
            }
        }
        auto changed = rstd::future::poll(self->changed, cx);
        if (ready || changed.is_ready()) return rstd::task::Poll<empty>::Ready(empty {});
        if (self->timer.is_some() && rstd::future::poll(*self->timer, cx).is_ready())
            return rstd::task::Poll<empty>::Ready(empty {});
        return rstd::task::Poll<empty>::Pending();
    }
};
struct H2WriteItem {
    Bytes                  bytes;
    u64                    serial;
    Option<Arc<BodyState>> body;
    usize                  payload_offset {}, payload_length {};
};
void h2_stop(H2Stream& stream) {
    if (stream.stopped) return;
    stream.stopped = true;
    stream.pulling = None();
    stream.pulled  = None();
    stream.pending = {};
    stream.work.request.body().cancel();
    if (stream.closing.is_none() && stream.closed.is_none())
        stream.closing = Some(stream.work.request.body().close().into_future());
}
auto h2_driver(Arc<NativeState> client, Arc<Slot> slot, Connection connection, Work initial)
    -> coro<empty> {
    lihttpto::h2::Limits limits;
    limits.header_bytes          = client->options.head_limit;
    limits.maximum_streams       = client->options.max_http2_streams;
    limits.receive_window        = client->options.receive_window;
    limits.maximum_informational = client->options.max_informational;
    auto created                 = lihttpto::h2::ClientConnection::make(limits);
    if (created.is_err()) {
        initial.body->fail(http2_error(rstd::move(created).unwrap_err()));
        initial.request.body().cancel();
        auto source = co_await initial.request.body().close();
        auto native = co_await connection.close();
        {
            auto body      = initial.body->fields.lock().unwrap();
            body->released = true;
            if (source.is_err())
                body->cleanup_error = Some(rstd::move(source).unwrap_err());
            else if (native.is_err())
                body->cleanup_error = Some(rstd::move(native).unwrap_err());
        }
        {
            auto state     = slot->fields.lock().unwrap();
            state->closing = state->done = true;
            state->active                = None();
        }
        initial.body->notify();
        slot->permit = None();
        client->changed.notify();
        co_return empty {};
    }
    auto               session = rstd::move(created).unwrap();
    Vec<Arc<H2Stream>> streams;
    streams.push(Arc<H2Stream>::make(rstd::move(initial), client->options.expect_continue_timeout));
    *streams[usize()]->work.body->connection_changed.lock().unwrap() = Some(slot->changed.clone());
    {
        auto s         = slot->fields.lock().unwrap();
        s->http2       = true;
        s->active      = None();
        s->http2_used  = usize(1);
        s->http2_limit = session.capacity();
        s->http2_bodies.push(streams[usize()]->work.body.clone());
    }
    client->changed.notify();
    H2Io             io;
    Vec<H2WriteItem> outgoing;
    Option<Error>    fatal;
    bool             closing = false, transport_failed = false;
    auto             settings_deadline = Instant::now() + client->options.connect_timeout;
    auto             idle_deadline     = Instant::now() + client->options.idle_timeout;
    Option<Instant>  shutdown_deadline;
    auto             find = [&](u32 id) -> H2Stream* {
        for (const auto& stream : streams)
            if (stream->id == id) return stream.as_ptr().as_raw_ptr();
        return nullptr;
    };
    auto queue_output = [&] {
        auto output = session.take_output();
        for (auto& item : output) {
            ++io.scheduled;
            Option<Arc<BodyState>> body;
            if (item.stream != u32())
                if (auto* s = find(item.stream)) {
                    s->last_serial = io.scheduled;
                    if (item.payload_length != usize()) body = Some(s->work.body.clone());
                }
            outgoing.push({ rstd::move(item.bytes),
                            io.scheduled,
                            rstd::move(body),
                            item.payload_offset,
                            item.payload_length });
        }
    };
    for (;;) {
        auto event    = slot->changed.listen();
        auto now      = Instant::now();
        bool progress = false;
        {
            auto s = slot->fields.lock().unwrap();
            if (s->closing && ! closing) {
                closing = true;
                session.shutdown();
            }
            for (auto& work : s->http2_pending) {
                *work.body->connection_changed.lock().unwrap() = Some(slot->changed.clone());
                streams.push(
                    Arc<H2Stream>::make(rstd::move(work), client->options.expect_continue_timeout));
                progress = true;
            }
            s->http2_pending.clear();
        }
        if (! session.established() && now >= settings_deadline && ! closing) {
            fatal   = Some(http2_error(lihttpto::h2::Error::Connection(u32(4))));
            closing = true;
            session.shutdown(u32(4));
        }
        if (io.written.is_some()) {
            auto result = io.written.take().unwrap();
            progress    = true;
            if (result.is_err()) {
                fatal            = Some(rstd::move(result).unwrap_err());
                transport_failed = true;
                closing          = true;
            } else
                io.completed = io.writing_serial;
        }
        if (io.received.is_some()) {
            auto result = io.received.take().unwrap();
            progress    = true;
            if (result.is_err()) {
                fatal            = Some(rstd::move(result).unwrap_err());
                transport_failed = true;
                closing          = true;
            } else if (result->is_empty()) {
                fatal            = Some(protocol_error("EOF in HTTP/2 connection"_Str));
                transport_failed = true;
                closing          = true;
            } else {
                auto received = session.feed(rstd::move(*result));
                if (received.is_err()) {
                    auto     error = rstd::move(received).unwrap_err();
                    unsigned code  = error.is_Compression() ? 9 : 1;
                    if (error.is_Connection()) code = error.as_Connection().code.to_primitive();
                    fatal   = Some(http2_error(rstd::move(error)));
                    closing = true;
                    session.shutdown(u32(code));
                }
            }
        }
        auto events = session.take_events();
        for (auto& item : events) {
            progress = true;
            RSTD_MATCH(item) {
                RSTD_CASE(Head, stream, head, informational) {
                    auto* s = find(stream);
                    if (! s) break;
                    {
                        auto b = s->work.body->fields.lock().unwrap();
                        if (informational) {
                            if (head.status.value() == u16(100)) b->continued = true;
                        } else
                            b->head = Some(rstd::move(head));
                    }
                    s->work.body->notify();
                    break;
                }
                RSTD_CASE(Data, stream, bytes) {
                    auto* s = find(stream);
                    if (! s) break;
                    usize         length = bytes.len();
                    Option<Error> error;
                    {
                        auto b    = s->work.body->fields.lock().unwrap();
                        auto sent = b->queue->try_send(rstd::move(bytes));
                        if (sent.is_err())
                            error = Some(rstd::move(sent).unwrap_err());
                        else
                            b->received_bytes += u64(length.to_primitive());
                    }
                    if (error.is_some()) {
                        s->work.body->fail(error.take().unwrap());
                        session.reset(stream);
                        h2_stop(*s);
                        s->local_done = s->remote_done = true;
                    } else {
                        s->buffered += length;
                        s->work.body->received_body(length);
                    }
                    s->work.body->notify();
                    break;
                }
                RSTD_CASE(Trailers, stream, headers) {
                    auto* s = find(stream);
                    if (s)
                        s->work.body->fields.lock().unwrap()->trailers = Some(rstd::move(headers));
                    break;
                }
                RSTD_CASE(End, stream) {
                    auto* s = find(stream);
                    if (! s) break;
                    s->remote_done = true;
                    {
                        auto b      = s->work.body->fields.lock().unwrap();
                        b->finished = true;
                        b->queue->finish();
                    }
                    if (! s->local_done) {
                        s->early      = true;
                        s->local_done = true;
                        session.reset(stream);
                        h2_stop(*s);
                    }
                    s->work.body->notify();
                    break;
                }
                RSTD_CASE(Reset, stream, code) {
                    auto* s = find(stream);
                    if (! s) break;
                    if (! s->early && ! (s->remote_done && code == u32()))
                        s->work.body->fail(
                            http2_error(lihttpto::h2::Error::Stream(u32(stream), u32(code))));
                    s->local_done = s->remote_done = true;
                    h2_stop(*s);
                    break;
                }
                RSTD_CASE(Goaway, last_stream, code) {
                    if (code != u32()) {
                        fatal   = Some(http2_error(lihttpto::h2::Error::Connection(u32(code))));
                        closing = true;
                    }
                    break;
                }
            }
        }
        Option<Instant> wake_at;
        auto            wake = [&](Instant at) {
            if (wake_at.is_none() || at < *wake_at) wake_at = Some(at);
        };
        if (closing) {
            if (shutdown_deadline.is_none())
                shutdown_deadline = Some(now + client->options.tls_close_timeout);
            if (now >= *shutdown_deadline) {
                transport_failed = true;
                if (fatal.is_none()) fatal = Some(Error::Timeout());
            } else
                wake(*shutdown_deadline);
        }
        if (! session.established() && ! closing) wake(settings_deadline);
        for (const auto& owned : streams) {
            auto& s = *owned;
            bool  canceled, consumed;
            {
                auto b         = s.work.body->fields.lock().unwrap();
                canceled       = b->canceled || b->error.is_some();
                consumed       = b->consumed;
                usize buffered = client->options.receive_window - b->queue->available();
                if (buffered < s.buffered) session.consume(s.id, s.buffered - buffered);
                s.buffered = buffered;
            }
            if (! canceled && s.work.body->deadline.is_some()) {
                if (now >= *s.work.body->deadline) {
                    s.work.body->cancel(StopReason::Deadline);
                    canceled = true;
                } else
                    wake(*s.work.body->deadline);
            }
            if (closing || canceled) {
                if (fatal.is_some())
                    s.work.body->fail(fatal->clone());
                else if (closing && ! consumed)
                    s.work.body->cancel(StopReason::OwnerClosed);
                if (! s.stopped) {
                    session.reset(s.id);
                    h2_stop(s);
                    s.local_done = s.remote_done = true;
                    progress                     = true;
                }
            }
            if (s.id == u32() && ! s.stopped && session.accepting()) {
                if (s.work.http2.is_none()) {
                    s.work.body->fail(Error::InvalidOptions("HTTP/2 preparation missing"_Str));
                    h2_stop(s);
                    s.local_done = s.remote_done = true;
                } else {
                    auto opened = session.open(*s.work.http2);
                    if (opened.is_ok()) {
                        s.id                                            = *opened;
                        s.work.body->fields.lock().unwrap()->dispatched = true;
                        progress                                        = true;
                    }
                }
            }
            if (! s.stopped && ! s.local_done && s.id != u32()) {
                bool proceed = true;
                if (s.work.request.parts().headers.contains("expect"_str)) {
                    auto b = s.work.body->fields.lock().unwrap();
                    if (! b->continued && now < s.continue_at) {
                        proceed = false;
                        wake(s.continue_at);
                    }
                }
                if (s.pulled.is_some()) {
                    progress  = true;
                    auto part = s.pulled.take().unwrap();
                    if (part.is_err()) {
                        s.work.body->fail(rstd::move(part).unwrap_err());
                        session.reset(s.id);
                        h2_stop(s);
                        s.local_done = s.remote_done = true;
                    } else if (part->is_none()) {
                        auto length = s.work.request.body().length();
                        if (length.is_some() && s.accepted != *length) {
                            s.work.body->fail(protocol_error("HTTP/2 upload length mismatch"_Str));
                            session.reset(s.id);
                            h2_stop(s);
                            s.local_done = s.remote_done = true;
                        } else
                            s.local_done = true;
                    } else {
                        auto bytes  = part->take().unwrap();
                        auto length = s.work.request.body().length();
                        if (bytes.len() > client->options.upload_window ||
                            (length.is_some() &&
                             u64(bytes.len().to_primitive()) > *length - s.accepted)) {
                            s.work.body->fail(
                                protocol_error("HTTP/2 upload exceeds window or length"_Str));
                            session.reset(s.id);
                            h2_stop(s);
                            s.local_done = s.remote_done = true;
                        } else {
                            s.accepted += u64(bytes.len().to_primitive());
                            {
                                auto b            = s.work.body->fields.lock().unwrap();
                                b->accepted_bytes = s.accepted;
                            }
                            s.pending = rstd::move(bytes);
                        }
                    }
                }
                if (! s.stopped && ! s.pending.is_empty()) {
                    auto count = rstd::cmp::min(s.pending.len(), session.writable(s.id));
                    if (count != usize()) {
                        auto sent = session.send(s.id, s.pending.slice(usize(), count));
                        if (sent.is_err()) {
                            s.work.body->fail(http2_error(rstd::move(sent).unwrap_err()));
                            h2_stop(s);
                            s.local_done = s.remote_done = true;
                        } else {
                            s.pending.advance(count);
                            progress = true;
                        }
                    }
                }
                auto length = s.work.request.body().length();
                if (! s.stopped && s.pending.is_empty() && length.is_some() &&
                    s.accepted == *length)
                    s.local_done = true;
                if (! s.stopped && s.local_done) {
                    auto trailers = s.work.request.body().take_trailers();
                    auto sent     = trailers.is_some() ? session.trailers(s.id, *trailers)
                                                       : session.send(s.id, Bytes {}, true);
                    if (sent.is_err())
                        s.work.body->fail(http2_error(rstd::move(sent).unwrap_err()));
                    s.work.body->fields.lock().unwrap()->upload_done = true;
                    if (s.closing.is_none() && s.closed.is_none())
                        s.closing = Some(s.work.request.body().close().into_future());
                    progress = true;
                } else if (! s.stopped && s.pulling.is_none() && s.pending.is_empty() && proceed) {
                    auto window =
                        rstd::cmp::min(session.writable(s.id), client->options.upload_window);
                    if (window != usize()) {
                        s.work.body->fields.lock().unwrap()->upload_pulled = true;
                        s.pulling = Some(s.work.request.body().next(window).into_future());
                    }
                }
            }
            if (s.closed.is_some()) {
                {
                    auto b           = s.work.body->fields.lock().unwrap();
                    b->upload_closed = true;
                    if (s.closed->is_err() && b->cleanup_error.is_none())
                        b->cleanup_error = Some(s.closed->as_ref().unwrap_err().clone());
                }
                if (s.closed->is_err()) {
                    auto state = client->fields.lock().unwrap();
                    if (state->close_error.is_none())
                        state->close_error = Some(s.closed->as_ref().unwrap_err().clone());
                }
            }
        }
        if (! session.accepting()) {
            auto state         = slot->fields.lock().unwrap();
            state->http2_limit = usize();
            for (const auto& s : streams)
                if (s->id == u32() && ! s->stopped) {
                    s->work.body->fail(http2_error(lihttpto::h2::Error::Stream(u32(), u32(7))));
                    h2_stop(*s);
                    s->local_done = s->remote_done = true;
                }
        }
        queue_output();
        if (transport_failed) {
            io.read  = None();
            io.write = None();
            outgoing.clear();
        } else {
            if (io.write.is_none() && ! outgoing.is_empty()) {
                auto item         = outgoing.remove(usize());
                io.writing_serial = item.serial;
                io.write =
                    Some((item.body.is_some() ? connection.send_body(rstd::move(item.bytes),
                                                                     item.body.take().unwrap(),
                                                                     item.payload_offset,
                                                                     item.payload_length)
                                              : connection.send(rstd::move(item.bytes)))
                             .into_future());
            }
            if (io.read.is_none() && ! closing)
                io.read = Some(connection.recv(usize(16384)).into_future());
        }
        for (usize i; i < streams.len();) {
            auto& s    = *streams[i];
            bool  done = false;
            {
                auto b = s.work.body->fields.lock().unwrap();
                done   = s.closed.is_some() && s.local_done && s.remote_done &&
                         (b->consumed || b->canceled || b->error.is_some()) &&
                         io.completed >= s.last_serial && ! transport_failed;
                if (done) b->released = true;
            }
            if (! done) {
                ++i;
                continue;
            }
            session.forget(s.id);
            s.work.body->notify();
            {
                auto state = slot->fields.lock().unwrap();
                --state->http2_used;
                for (usize j; j < state->http2_bodies.len(); ++j)
                    if (Arc<BodyState>::ptr_eq(state->http2_bodies[j], s.work.body)) {
                        state->http2_bodies.remove(j);
                        break;
                    }
            }
            streams.remove(i);
            progress = true;
            client->changed.notify();
        }
        bool empty = streams.is_empty();
        {
            auto state         = slot->fields.lock().unwrap();
            state->http2_limit = session.accepting() ? session.capacity() : usize();
            if (! empty) idle_deadline = now + client->options.idle_timeout;
            state->idle = empty && ! closing;
            empty       = empty && state->http2_pending.is_empty();
        }
        if (empty && ! closing) {
            auto  state = client->fields.lock().unwrap();
            usize idle, same_idle;
            for (const auto& other : state->slots) {
                auto entry = other->fields.lock().unwrap();
                if (entry->idle) {
                    ++idle;
                    if (other->origin == slot->origin) ++same_idle;
                }
            }
            if (state->closed || idle > client->options.max_idle ||
                same_idle > client->options.max_idle_per_origin) {
                closing = true;
                session.shutdown();
                progress           = true;
                auto entry         = slot->fields.lock().unwrap();
                entry->closing     = true;
                entry->idle        = false;
                entry->http2_limit = usize();
            }
        }
        client->changed.notify();
        if (transport_failed) {
            bool closed = true;
            for (const auto& s : streams)
                if (s->closed.is_none()) closed = false;
            if (closed) break;
        }
        if (empty && ! closing && now >= idle_deadline) {
            closing = true;
            session.shutdown();
            progress = true;
        }
        queue_output();
        if (empty && (! session.accepting() || closing)) {
            if (io.write.is_none() && outgoing.is_empty()) break;
        }
        if (empty && ! closing) wake(idle_deadline);
        if (progress) continue;
        co_await H2Wake { &io,
                          &streams,
                          rstd::move(event),
                          wake_at.is_some() ? Some(rstd::async::into_future(
                                                  rstd::async::sleep(remaining(*wake_at))))
                                            : None<H2Timer>() };
    }
    io.read  = None();
    io.write = None();
    {
        auto s     = slot->fields.lock().unwrap();
        s->closing = true;
        s->idle    = false;
    }
    auto closed = co_await connection.close();
    if (closed.is_err()) {
        auto state = client->fields.lock().unwrap();
        if (state->close_error.is_none())
            state->close_error = Some(closed.as_ref().unwrap_err().clone());
    }
    for (const auto& stream : streams) {
        auto b      = stream->work.body->fields.lock().unwrap();
        b->released = true;
        if (closed.is_err()) b->cleanup_error = Some(closed.as_ref().unwrap_err().clone());
        stream->work.body->notify();
    }
    Vec<Work> pending;
    {
        auto state = slot->fields.lock().unwrap();
        pending    = rstd::move(state->http2_pending);
        state->http2_bodies.clear();
        state->http2_used  = usize();
        state->http2_limit = usize();
    }
    for (auto& work : pending) {
        work.body->fail(Error::Closed());
        work.request.body().cancel();
        auto released = co_await work.request.body().close();
        {
            auto b      = work.body->fields.lock().unwrap();
            b->released = true;
            if (released.is_err()) b->cleanup_error = Some(rstd::move(released).unwrap_err());
        }
        work.body->notify();
    }
    {
        auto state  = slot->fields.lock().unwrap();
        state->done = true;
    }
    slot->permit = None();
    client->changed.notify();
    co_return empty {};
}
} // namespace lifetchto::http
