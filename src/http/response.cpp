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
        if (f->error.is_none()) f->error = Some(stop_error(reason));
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
        if (f->error.is_some()) co_return Err(f->error->clone());
        if (f->finished) co_return Ok(None());
        if (f->canceled) co_return Err(Error::Canceled());
        f->reading = true;
    }
    ResponseReadGuard guard { state.clone() };
    auto              result = co_await state->source->next();
    {
        auto f = state->fields.lock().unwrap();
        if (result.is_err()) {
            if (f->error.is_none())
                f->error = Some(result.as_ref().unwrap_err().clone());
            else if (result.as_ref().unwrap_err().is_Canceled() ||
                     result.as_ref().unwrap_err().is_Closed())
                result = Err(f->error->clone());
            else
                f->error = Some(result.as_ref().unwrap_err().clone());
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
        if (f->error.is_some()) return Err(f->error->clone());
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
            if (f->error.is_some()) co_return Err(f->error->clone());
            if (f->canceled) co_return Err(Error::Canceled());
            if (f->finished) co_return Ok(empty {});
        }
        co_await rstd::move(event);
    }
}
auto close_response(Arc<ResponseReadState> state) -> coro<Result<empty>> {
    auto gate = co_await state->close_gate.lock();
    if (! state->closed) {
        if (state->close_task.is_none())
            state->close_task = Some(state->source->close().into_future());
        auto result       = co_await rstd::future::by_ref(*state->close_task);
        state->close_task = None();
        state->closed     = true;
        if (result.is_err()) state->close_error = Some(rstd::move(result).unwrap_err());
    }
    if (state->close_error.is_some()) co_return Err(state->close_error->clone());
    co_return Ok(empty {});
}
auto BodyControl::wait() const -> coro<Result<empty>> {
    return wait_response(state_.clone());
}
auto BodyControl::close() const -> coro<Result<empty>> {
    cancel();
    return close_response(state_.clone());
}
} // namespace lifetchto::http
