module lifetchto.http;
import :state;
using namespace rstd::prelude;

namespace lifetchto::http
{
void BodyState::record_cleanup(Result<empty> result) const {
    if (result.is_err())
        fields.lock().unwrap()->cleanup_errors.push(rstd::move(result).unwrap_err());
}

void BodyState::notify() const {
    changed.notify();
    auto event = connection_changed.lock().unwrap();
    if (event->is_some()) (*event)->notify();
}

void BodyState::set_connect_stage(ConnectStage stage) const {
    {
        auto f           = fields.lock().unwrap();
        f->connect_stage = stage;
    }
    changed.notify();
}

void BodyState::fail(Error e) {
    bool changed = false;
    {
        auto f = fields.lock().unwrap();
        if (! f->failed) {
            changed    = true;
            f->failed  = true;
            f->failure = Some(report_failure(e));
            // The queue wakes readers; the body owns the original failure.
            if (f->queue.is_some()) f->queue->fail(Error::Closed());
            f->error = Some(rstd::move(e));
        }
    }
    if (changed) notify();
}

void BodyState::cancel(StopReason reason) {
    bool changed = false;
    {
        auto f      = fields.lock().unwrap();
        changed     = ! f->canceled || (reason == StopReason::Deadline && ! f->failed);
        f->canceled = true;
        if (reason == StopReason::Deadline && ! f->failed) {
            f->failed  = true;
            f->error   = Some(Error::Timeout());
            f->failure = Some(report_failure(*f->error));
        }
        if (changed && f->queue.is_some()) f->queue->fail(stop_error(reason));
    }
    if (changed) notify();
}

auto active_duplex(NativeFields& fields) -> usize {
    usize count {};
    for (usize i {}; i < fields.duplex.len();) {
        auto lease = fields.duplex[i].lease.upgrade();
        if (! lease || lease->is_released()) {
            fields.duplex.remove(i);
            continue;
        }
        if (! fields.duplex[i].native_done->load()) ++count;
        ++i;
    }
    return count;
}

auto body_stop(Arc<BodyState> body, bool writing) -> coro<Error> {
    for (;;) {
        auto event = body->changed.listen();
        {
            auto f = body->fields.lock().unwrap();
            if (f->failed) co_return Error::Closed();
            if (f->canceled || (writing && f->stop_upload)) co_return Error::Canceled();
        }
        if (body->deadline.is_some()) {
            auto ready = co_await rstd::async::select(
                rstd::move(event), rstd::async::sleep(remaining(*body->deadline)));
            if (ready.is_right()) co_return Error::Timeout();
        } else
            co_await rstd::move(event);
    }
}

auto connect_stop(Arc<BodyState> body, Instant deadline) -> coro<Error> {
    auto result = co_await rstd::async::select(body_stop(body.clone()).into_future(),
                                               rstd::async::sleep(remaining(deadline)));
    if (result.is_left()) co_return rstd::move(result).unwrap_left();
    co_return Error::Timeout();
}
} // namespace lifetchto::http
