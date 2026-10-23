module lifetchto.http:native_body;
import :state;
using namespace rstd::prelude;
namespace lifetchto::http
{
struct NativeReadGuard {
    Arc<BodyState> state;
    bool           completed {};
    ~NativeReadGuard() {
        {
            auto f     = state->fields.lock().unwrap();
            f->reading = false;
            if (! completed) f->canceled = true;
        }
        state->changed.notify();
    }
};
auto read_body(Arc<BodyState> state) -> coro<Result<Option<Bytes>>> {
    Option<ByteQueue> queue;
    {
        auto f = state->fields.lock().unwrap();
        if (f->error.is_some()) co_return Err(f->error->clone());
        if (f->canceled) co_return Err(Error::Canceled());
        if (f->consumed) co_return Ok(None());
        queue = Some(f->queue->clone());
    }
    auto part = co_await queue->next();
    if (part.is_err()) state->fail(part.as_ref().unwrap_err().clone());
    if (part.is_ok() && part->is_none()) {
        auto f      = state->fields.lock().unwrap();
        f->consumed = true;
    }
    state->changed.notify();
    co_return rstd::move(part);
}
auto wait_body_release(Arc<BodyState> state) -> coro<Result<empty>> {
    for (;;) {
        auto event = state->changed.listen();
        {
            auto f = state->fields.lock().unwrap();
            if (f->released) {
                if (f->cleanup_error.is_some()) co_return Err(f->cleanup_error->clone());
                co_return Ok(empty {});
            }
        }
        co_await rstd::move(event);
    }
}
struct NativeResponseSource {
    Arc<BodyState> state;
    auto           next() const -> coro<Result<Option<Bytes>>> { return read_body(state.clone()); }
    auto           trailers() const -> Result<Option<lihttpto::Headers>> {
        auto f = state->fields.lock().unwrap();
        if (f->error.is_some()) return Err(f->error->clone());
        if (! f->consumed) return Ok(None());
        return Ok(f->trailers.clone());
    }
    void cancel() const {
        {
            auto f = state->fields.lock().unwrap();
            if (f->consumed || f->released) return;
        }
        state->cancel();
    }
    auto close() const -> coro<Result<empty>> {
        cancel();
        return wait_body_release(state.clone());
    }
};
} // namespace lifetchto::http
namespace rstd
{
template<>
struct Impl<lifetchto::http::ResponseSource, lifetchto::http::NativeResponseSource>
    : ImplBase<lifetchto::http::NativeResponseSource> {
    auto next() const { return this->self().next(); }
    auto trailers() const { return this->self().trailers(); }
    void cancel() const { this->self().cancel(); }
    auto close() const { return this->self().close(); }
};
} // namespace rstd
namespace lifetchto::http
{
auto native_body(Arc<BodyState> state) -> ResponseBody {
    return ResponseBody::from_source(
        Arc<rstd::dyn<ResponseSource>>::make(NativeResponseSource { rstd::move(state) }));
}
} // namespace lifetchto::http
