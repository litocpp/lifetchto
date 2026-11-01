module lifetchto.http:driver;
import :connection;
#if LITO_FEAT_HTTP2
import :h2_driver;
#endif
import :state;
using namespace rstd::prelude;
using namespace rstd::literals;
namespace lifetchto::http
{
auto upload(Connection& connection, Work& work, const TransportOptions& options)
    -> coro<Result<empty>> {
    auto head = work.encoder.take_head();
    if (head.is_err())
        co_return Err(
            protocol_error(String::make("request head state"_str), None<lihttpto::DecodeError>()));
    {
        auto f        = work.body->fields.lock().unwrap();
        f->dispatched = true;
    }
    auto sent =
        co_await guarded(work.body.clone(), connection.send(rstd::move(head).unwrap()), true);
    if (sent.is_err()) co_return Err(rstd::move(sent).unwrap_err());
    if (work.request.parts().headers.contains("expect"_str)) {
        auto until = Instant::now() + options.expect_continue_timeout;
        if (work.body->deadline.is_some() && *work.body->deadline < until)
            until = *work.body->deadline;
        for (;;) {
            auto event = work.body->changed.listen();
            {
                auto f = work.body->fields.lock().unwrap();
                if (f->canceled || f->error.is_some()) co_return Err(Error::Canceled());
                if (f->continued || f->stop_upload) break;
            }
            auto result = co_await rstd::async::select(rstd::move(event),
                                                       rstd::async::sleep(remaining(until)));
            if (result.is_right()) break;
        }
    }
    u64  count {};
    auto length = work.request.body().length();
    while (length.is_none() || count < *length) {
        {
            auto f           = work.body->fields.lock().unwrap();
            f->upload_pulled = true;
        }
        auto part = co_await guarded(
            work.body.clone(), work.request.body().next(options.upload_window), true);
        if (part.is_err()) co_return Err(rstd::move(part).unwrap_err());
        if (part->is_none()) break;
        auto bytes = part->take().unwrap();
        if (bytes.is_empty()) {
            co_await rstd::async::yield_now();
            continue;
        }
        if (bytes.len() > options.upload_window) co_return Err(Error::TooLarge());
        count += u64(bytes.len().to_primitive());
        {
            auto f            = work.body->fields.lock().unwrap();
            f->accepted_bytes = count;
        }
        auto encoded = work.encoder.encode_piece(lihttpto::BodyFrame::Data(rstd::move(bytes)));
        if (encoded.is_err())
            co_return Err(protocol_error(String::make("request body length mismatch"_str),
                                         None<lihttpto::DecodeError>()));
        auto piece = rstd::move(encoded).unwrap();
        auto sent  = co_await guarded(work.body.clone(),
                                      connection.send_body(rstd::move(piece.bytes),
                                                           work.body.clone(),
                                                           piece.payload_offset,
                                                           piece.payload_length),
                                      true);
        if (sent.is_err()) co_return Err(rstd::move(sent).unwrap_err());
    }
    auto trailers = work.request.body().take_trailers();
    if (trailers.is_some()) {
        auto encoded = work.encoder.encode(lihttpto::BodyFrame::Trailers(trailers.take().unwrap()));
        if (encoded.is_err())
            co_return Err(protocol_error(String::make("invalid upload trailers"_str),
                                         None<lihttpto::DecodeError>()));
        auto sent = co_await guarded(
            work.body.clone(), connection.send(rstd::move(encoded).unwrap()), true);
        if (sent.is_err()) co_return Err(rstd::move(sent).unwrap_err());
    }
    auto last = work.encoder.finish();
    if (last.is_err())
        co_return Err(protocol_error(String::make("request body ended before Content-Length"_str),
                                     None<lihttpto::DecodeError>()));
    if (! last->is_empty()) {
        auto sent =
            co_await guarded(work.body.clone(), connection.send(rstd::move(last).unwrap()), true);
        if (sent.is_err()) co_return Err(rstd::move(sent).unwrap_err());
    }
    {
        auto f         = work.body->fields.lock().unwrap();
        f->upload_done = true;
    }
    work.body->notify();
    co_return Ok(empty {});
}
auto send_request(Connection& connection, Work& work, const TransportOptions& options)
    -> coro<Result<empty>> {
    auto result = co_await upload(connection, work, options);
    bool early;
    {
        auto f = work.body->fields.lock().unwrap();
        early  = f->stop_upload;
    }
    if (result.is_err() && ! early) work.body->fail(result.unwrap_err().clone());
    if (result.is_err()) work.request.body().cancel();
    auto closed = co_await work.request.body().close();
    {
        auto f           = work.body->fields.lock().unwrap();
        f->upload_closed = true;
    }
    if (closed.is_err()) {
        auto f           = work.body->fields.lock().unwrap();
        f->cleanup_error = Some(closed.unwrap_err().clone());
    }
    if (closed.is_err() && result.is_ok()) {
        work.body->fail(closed.unwrap_err().clone());
        co_return Err(rstd::move(closed).unwrap_err());
    }
    co_return rstd::move(result);
}
auto read_response(Connection&                                      connection,
                   Work&                                            work,
                   const TransportOptions&                          options,
                   Option<rstd::async::coro<Result<Bytes>>::Future> initial) -> coro<Result<bool>> {
    lihttpto::ResponseHeadDecoder head(
        work.request.parts().method.clone(), options.head_limit, options.max_informational);
    Option<rstd::boxed::Box<lihttpto::BodyDecoder>> decoder;
    Bytes                                           bytes;
    bool                                            reusable = false;
    for (;;) {
        auto  event = work.body->changed.listen();
        usize room;
        {
            auto f = work.body->fields.lock().unwrap();
            room   = f->queue->available();
        }
        if (room == usize()) {
            auto ready = co_await rstd::async::select(rstd::move(event),
                                                      body_stop(work.body.clone()).into_future());
            if (ready.is_right()) co_return Err(rstd::move(ready).unwrap_right());
            continue;
        }
        Result<Bytes> received = Err(Error::Canceled());
        if (initial.is_some()) {
            auto ready = co_await rstd::async::select(rstd::future::by_ref(*initial),
                                                      body_stop(work.body.clone()).into_future());
            if (ready.is_right()) co_return Err(rstd::move(ready).unwrap_right());
            received = rstd::move(ready).unwrap_left();
            initial  = None();
        } else
            received = co_await guarded(work.body.clone(), connection.recv(room));
        if (received.is_err()) co_return Err(rstd::move(received).unwrap_err());
        bytes = rstd::move(received).unwrap();
        {
            auto f = work.body->fields.lock().unwrap();
            f->received_bytes += u64(bytes.len().to_primitive());
        }
        bool eof = bytes.is_empty();
        if (eof && decoder.is_none())
            co_return Err(protocol_error(String::make("EOF before final response head"_str),
                                         None<lihttpto::DecodeError>()));
        while (! bytes.is_empty()) {
            if (decoder.is_none()) {
                auto parsed = head.feed(bytes.as_slice());
                if (parsed.is_err())
                    co_return Err(protocol_error(String::make("invalid HTTP response head"_str),
                                                 Some(parsed.unwrap_err())));
                if (parsed->is_none()) {
                    bytes = {};
                    break;
                }
                auto decoded = parsed->take().unwrap();
                bytes.advance(decoded.input_consumed);
                if (decoded.informational) {
                    if (decoded.head.status.value() == u16(100)) {
                        {
                            auto f       = work.body->fields.lock().unwrap();
                            f->continued = true;
                        }
                        work.body->notify();
                    }
                    continue;
                }
                lihttpto::BodyLimits limits;
                limits.data_bytes     = u64::MAX;
                limits.metadata_bytes = options.head_limit - head.metadata_bytes();
                decoder =
                    Some(rstd::boxed::Box<lihttpto::BodyDecoder>::make(decoded.framing, limits));
                reusable = decoded.keep_alive && work.encoder.keep_alive();
                {
                    auto f         = work.body->fields.lock().unwrap();
                    f->stop_upload = ! f->upload_done;
                    f->reusable    = reusable;
                    f->head        = Some(rstd::move(decoded.head));
                }
                work.body->notify();
            }
            if (decoder.is_some()) {
                auto progress = (*decoder)->feed(bytes.as_slice());
                if (progress.is_err())
                    co_return Err(protocol_error(String::make("invalid HTTP response body"_str),
                                                 Some(progress.unwrap_err())));
                if (! progress->data.is_empty()) {
                    auto offset = usize(static_cast<rstd::size_t>(progress->data.as_raw_ptr() -
                                                                  bytes.as_slice().as_raw_ptr()));
                    auto chunk  = bytes.slice(offset, offset + progress->data.len());
                    {
                        auto f      = work.body->fields.lock().unwrap();
                        auto queued = f->queue->try_send(rstd::move(chunk));
                        if (queued.is_err()) co_return Err(rstd::move(queued).unwrap_err());
                    }
                    work.body->received_body(progress->data.len());
                    work.body->notify();
                }
                bytes.advance(progress->consumed);
                if (progress->status == lihttpto::DecodeStatus::Complete) {
                    if (! bytes.is_empty())
                        co_return Err(protocol_error(
                            String::make("unsolicited bytes after HTTP response"_str),
                            None<lihttpto::DecodeError>()));
                    {
                        auto f      = work.body->fields.lock().unwrap();
                        f->trailers = (*decoder)->take_trailers();
                        f->finished = true;
                        f->queue->finish();
                    }
                    work.body->notify();
                    co_return Ok(reusable);
                }
            }
        }
        if (decoder.is_some()) {
            auto progress = (*decoder)->feed({});
            if (progress.is_err())
                co_return Err(protocol_error(String::make("invalid HTTP body state"_str),
                                             Some(progress.unwrap_err())));
            if (progress->status == lihttpto::DecodeStatus::Complete || eof) {
                if (eof && (*decoder)->finish().is_err())
                    co_return Err(protocol_error(String::make("truncated HTTP response body"_str),
                                                 Some(lihttpto::DecodeError::Truncated)));
                {
                    auto f      = work.body->fields.lock().unwrap();
                    f->trailers = (*decoder)->take_trailers();
                    f->finished = true;
                    f->queue->finish();
                }
                work.body->notify();
                co_return Ok(reusable && ! eof);
            }
        }
    }
}
auto receive_response(Connection&                                      connection,
                      Work&                                            work,
                      const TransportOptions&                          options,
                      Option<rstd::async::coro<Result<Bytes>>::Future> initial)
    -> coro<Result<bool>> {
    auto result = co_await read_response(connection, work, options, rstd::move(initial));
    if (result.is_err()) work.body->fail(result.unwrap_err().clone());
    co_return rstd::move(result);
}
template<class F>
struct ReadProbe {
    F* future;
    using Output = Option<rstd::future::future_output_t<F>>;
    auto poll(mut_ref<ReadProbe> self, rstd::task::Context& cx) -> rstd::task::Poll<Output> {
        auto result = rstd::future::poll(*self->future, cx);
        if (result.is_pending()) return rstd::task::Poll<Output>::Ready(None());
        return rstd::task::Poll<Output>::Ready(Some(rstd::move(result).take()));
    }
};
auto driver(Arc<NativeState> client, Arc<Slot> slot) -> coro<empty> {
    auto work = [&]() {
        auto f = slot->fields.lock().unwrap();
        return f->pending.take().unwrap();
    }();
    auto connected = co_await connect_to(
        client.clone(), slot.clone(), work.request.parts().url, work.body.clone());
    if (connected.is_err()) {
        work.body->fail(rstd::move(connected).unwrap_err());
        work.request.body().cancel();
        auto source_closed = co_await work.request.body().close();
        if (source_closed.is_err()) {
            auto f = client->fields.lock().unwrap();
            if (f->close_error.is_none())
                f->close_error = Some(rstd::move(source_closed).unwrap_err());
        }
        {
            auto f    = slot->fields.lock().unwrap();
            f->done   = true;
            f->active = None();
        }
        {
            auto f      = work.body->fields.lock().unwrap();
            f->released = true;
            if (source_closed.is_err())
                f->cleanup_error = Some(source_closed.as_ref().unwrap_err().clone());
        }
        work.body->notify();
        slot->permit = None();
        client->changed.notify();
        co_return empty {};
    }
    auto connection = rstd::move(connected).unwrap();
#if LITO_FEAT_HTTP2
    if (connection.http2) {
        co_await h2_driver(client.clone(), slot.clone(), rstd::move(connection), rstd::move(work));
        co_return empty {};
    }
#endif
    Option<rstd::async::coro<Result<Bytes>>::Future> idle_read;
    bool                                             idle_failure = false;
    for (;;) {
        auto outcomes = co_await rstd::async::join(
            send_request(connection, work, client->options).into_future(),
            receive_response(connection, work, client->options, rstd::move(idle_read))
                .into_future());
        auto written  = rstd::move(rstd::get<0>(outcomes));
        auto received = rstd::move(rstd::get<1>(outcomes));
        bool reusable = written.is_ok() && received.is_ok() && *received;
        while (reusable) {
            auto event = work.body->changed.listen();
            {
                auto f = work.body->fields.lock().unwrap();
                if (f->canceled || f->error.is_some() || f->stop_upload) {
                    reusable = false;
                    break;
                }
                if (f->consumed) break;
            }
            auto result = co_await rstd::async::select(rstd::move(event),
                                                       body_stop(work.body.clone()).into_future());
            if (result.is_right()) {
                work.body->fail(rstd::move(result).unwrap_right());
                reusable = false;
            }
        }
        if (! reusable) break;
        {
            auto  f = client->fields.lock().unwrap();
            usize idle {}, same_idle {};
            for (const auto& entry : f->slots) {
                auto s = entry->fields.lock().unwrap();
                if (s->idle) {
                    ++idle;
                    if (entry->origin == slot->origin) ++same_idle;
                }
            }
            auto s = slot->fields.lock().unwrap();
            if (f->closed || s->closing || idle >= client->options.max_idle ||
                same_idle >= client->options.max_idle_per_origin)
                reusable = false;
            else {
                s->active = None();
                s->idle   = true;
            }
        }
        client->changed.notify();
        if (! reusable) break;
        {
            auto f      = work.body->fields.lock().unwrap();
            f->released = true;
        }
        work.body->notify();
        idle_read          = Some(connection.recv(client->options.receive_window).into_future());
        auto idle_deadline = Instant::now() + client->options.idle_timeout;
        bool checkout      = false;
        for (;;) {
            auto event = slot->changed.listen();
            {
                auto s = slot->fields.lock().unwrap();
                if (s->closing) break;
                if (s->pending.is_some()) {
                    checkout = true;
                    break;
                }
            }
            auto command = rstd::async::select(rstd::move(event),
                                               rstd::async::sleep(remaining(idle_deadline)));
            auto result =
                co_await rstd::async::select(rstd::future::by_ref(*idle_read), rstd::move(command));
            if (result.is_left()) {
                idle_failure = true;
                break;
            }
            if (rstd::move(result).unwrap_right().is_right()) break;
        }
        if (! checkout) break;
        auto prior = co_await ReadProbe { rstd::addressof(*idle_read) };
        if (prior.is_some()) {
            idle_failure = true;
            idle_read    = None();
            break;
        }
        {
            auto s = slot->fields.lock().unwrap();
            work   = s->pending.take().unwrap();
        }
    }
    idle_read = None();
    {
        auto s     = slot->fields.lock().unwrap();
        s->idle    = false;
        s->closing = true;
    }
    auto released = co_await connection.close();
    {
        auto f = work.body->fields.lock().unwrap();
        if (released.is_err()) f->cleanup_error = Some(released.as_ref().unwrap_err().clone());
        f->released = true;
    }
    work.body->notify();
    Option<Work> pending;
    {
        auto s    = slot->fields.lock().unwrap();
        pending   = s->pending.take();
        s->active = None();
        s->done   = true;
    }
    if (pending.is_some()) {
        pending->body->fail(Error::Closed());
        pending->request.body().cancel();
        auto closed = co_await pending->request.body().close();
        {
            auto f      = pending->body->fields.lock().unwrap();
            f->released = true;
            if (closed.is_err()) f->cleanup_error = Some(rstd::move(closed).unwrap_err());
        }
        pending->body->notify();
    }
    slot->permit = None();
    client->changed.notify();
    co_return empty {};
}
} // namespace lifetchto::http
