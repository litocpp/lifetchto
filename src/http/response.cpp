module lifetchto.http;
import :response;
using namespace rstd::prelude;
namespace lifetchto::http
{
void cancel_response(const Arc<ResponseReadState>& state, StopReason reason) {
    {
        auto f = state->fields.lock().unwrap();
        if (f->finished || f->canceled) return;
        f->canceled = true;
        if (f->failure.is_none()) {
            f->stopped = Some(reason);
            f->failure = Some(report_failure(stop_error(reason)));
        }
    }
    state->source->cancel();
    state->changed.notify();
}
struct ResponseReadGuard {
    Arc<ResponseReadState> state;
    bool                   complete {};
    ~ResponseReadGuard() {
        {
            auto f     = state->fields.lock().unwrap();
            f->reading = false;
        }
        if (! complete) cancel_response(state);
    }
};
auto next_response(Arc<ResponseReadState> state) -> coro<Result<Option<Bytes>>> {
    {
        auto f = state->fields.lock().unwrap();
        if (f->reading) co_return Err(Error::Busy());
        if (f->failure.is_some())
            co_return Err(f->stopped.is_some() ? stop_error(*f->stopped) : Error::Closed());
        if (f->finished) co_return Ok(None());
        if (f->canceled) co_return Err(Error::Canceled());
        f->reading = true;
    }
    ResponseReadGuard guard { state.clone() };
    auto              result = co_await state->source->next();
    {
        auto f = state->fields.lock().unwrap();
        if (result.is_err()) {
            if (f->stopped.is_some() && (result.as_ref().unwrap_err().is_Canceled() ||
                                         result.as_ref().unwrap_err().is_Closed()))
                result = Err(stop_error(*f->stopped));
            else
                f->stopped = None();
            f->failure = Some(report_failure(result.as_ref().unwrap_err()));
        } else if (result->is_none())
            f->finished = true;
    }
    guard.complete = true;
    state->changed.notify();
    co_return rstd::move(result);
}
auto ResponseBody::next() -> coro<Result<Option<Bytes>>> {
    return next_response(state_.clone());
}
auto ResponseBody::trailers() const -> Result<Option<lihttpto::Headers>> {
    {
        auto f = state_->fields.lock().unwrap();
        if (f->failure.is_some()) return Err(Error::Closed());
        if (f->canceled) return Err(Error::Canceled());
        if (! f->finished) return Ok(None());
    }
    return state_->source->trailers();
}
auto wait_response(Arc<ResponseReadState> state) -> coro<Result<empty>> {
    for (;;) {
        auto event = state->changed.listen();
        {
            auto f = state->fields.lock().unwrap();
            if (f->failure.is_some())
                co_return Err(f->stopped.is_some() ? stop_error(*f->stopped) : Error::Closed());
            if (f->canceled) co_return Err(Error::Canceled());
            if (f->finished) co_return Ok(empty {});
        }
        co_await rstd::move(event);
    }
}
auto close_response(Arc<ResponseReadState> state) -> coro<CleanupReport> {
    auto gate = co_await state->close_gate.lock();
    if (! state->closed) {
        if (state->close_task.is_none())
            state->close_task = Some(state->source->close().into_future());
        auto result       = co_await rstd::future::by_ref(*state->close_task);
        state->close_task = None();
        state->closed     = true;
        auto f            = state->cleanup.lock().unwrap();
        if (result.is_err()) f->report.add(report_failure(result.as_ref().unwrap_err()));
        if (! f->abandoned) f->result = Some(rstd::move(result));
    }
    co_return state->cleanup.lock().unwrap()->report.clone();
}
auto BodyControl::wait() const -> coro<Result<empty>> {
    return wait_response(state_.clone());
}
auto BodyControl::failure() const -> Option<FailureReport> {
    auto f = state_->fields.lock().unwrap();
    return f->failure.is_some() ? Some(f->failure->clone()) : None<FailureReport>();
}
auto BodyControl::close() const -> coro<CleanupReport> {
    cancel();
    return close_response(state_.clone());
}
auto release_response(Arc<ResponseReadState> state) -> coro<Result<empty>> {
    cancel_response(state);
    (void)co_await close_response(state.clone());
    auto result = state->cleanup.lock().unwrap()->result.take();
    if (result.is_none()) co_return Err(Error::Contract("body cleanup result already taken"_Str));
    co_return result.take().unwrap();
}
auto BodyCleanup::close() const -> coro<Result<empty>> {
    return release_response(state_.clone());
}
} // namespace lifetchto::http
