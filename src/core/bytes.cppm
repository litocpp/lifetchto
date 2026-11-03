export module lifetchto.core:bytes;
export import :engine;
using namespace rstd::prelude;
using namespace rstd::literals;
namespace lifetchto
{
export struct ByteSource {
    template<class Self, class = void>
    struct Api {
        using Trait = ByteSource;
        auto next(usize window) const -> coro<Result<Option<Bytes>>> {
            return rstd::trait_call<0>(this, window);
        }
        void request_stop(StopReason reason) const { rstd::trait_call<1>(this, reason); }
        auto release(ReleaseContext context) const -> coro<Result<rstd::empty>> {
            return rstd::trait_call<2>(this, context);
        }
    };
    template<class T>
    using Funcs = rstd::TraitFuncs<&T::next, &T::request_stop, &T::release>;
};
template<class S>
struct StoredByteSource {
    Arc<S> source;
    auto   next(usize maximum) const -> coro<Result<Option<Bytes>>> {
        auto result = [&] {
            if constexpr (requires { source->next(maximum); })
                return source->next(maximum);
            else
                return source->next();
        }();
        auto part = co_await rstd::move(result);
        if (part.is_ok() && part->is_some() && (**part).len() > maximum)
            co_return Err(Error::TooLarge());
        co_return rstd::move(part);
    }
    void request_stop(StopReason reason) const {
        if constexpr (requires { source->request_stop(reason); })
            source->request_stop(reason);
        else if constexpr (requires { source->cancel(); })
            source->cancel();
    }
    auto release(ReleaseContext context) const -> coro<Result<rstd::empty>> {
        if constexpr (requires { source->release(context); })
            co_return co_await source->release(context);
        else if constexpr (requires { source->close(); })
            co_return co_await source->close();
        else
            co_return Ok(empty {});
    }
};
struct UploadSourceState {
    Arc<rstd::dyn<ByteSource>>          source;
    rstd::async::Mutex<empty>           close_gate { empty {} };
    Option<coro<Result<empty>>::Future> release;
    CloseOutcome                        outcome;
    bool                                closed {};
    explicit UploadSourceState(Arc<rstd::dyn<ByteSource>> source): source(rstd::move(source)) {}
};
export class UploadBody {
    Option<Bytes>                  bytes_;
    Option<Arc<UploadSourceState>> source_;
    Option<u64>                    length_ { Some(u64()) };

public:
    UploadBody() = default;
    UploadBody(UploadBody&& other) noexcept
        : bytes_(other.bytes_.take()), source_(other.source_.take()), length_(other.length_) {}
    UploadBody(const UploadBody&) = delete;
    auto operator=(UploadBody&& other) noexcept -> UploadBody& {
        if (this != &other) {
            cancel();
            bytes_  = rstd::move(other.bytes_);
            source_ = other.source_.take();
            length_ = other.length_;
        }
        return *this;
    }
    ~UploadBody() { cancel(); }
    static auto bytes(Bytes bytes) -> UploadBody {
        UploadBody result;
        result.length_ = Some(u64(bytes.len().to_primitive()));
        result.bytes_  = Some(rstd::move(bytes));
        return result;
    }
    static auto source(Arc<rstd::dyn<ByteSource>> source, Option<u64> length = {}) -> UploadBody {
        UploadBody result;
        result.source_ = Some(Arc<UploadSourceState>::make(rstd::move(source)));
        result.length_ = length;
        return result;
    }
    template<class S>
    static auto from_source(S source, Option<u64> length = {}) -> UploadBody;
    auto        length() const -> Option<u64> { return length_; }
    auto        next(usize maximum = usize(65536)) -> coro<Result<Option<Bytes>>>;
    void        cancel() const {
        if (source_.is_some()) (*source_)->source->request_stop(StopReason::Abandoned);
    }
    void request_close() const { cancel(); }
    auto close() const -> coro<Result<rstd::empty>>;
    auto wait_closed() const -> coro<CleanupReport>;
    auto take_close_result() const -> Option<Result<empty>> {
        return source_.is_some() ? (*source_)->outcome.take_result()
                                 : Some(Result<empty>(Ok(empty {})));
    }
};
auto source_next(Arc<UploadSourceState> source, usize maximum) -> coro<Result<Option<Bytes>>> {
    co_return co_await source->source->next(maximum);
}
auto memory_next(Option<Bytes> bytes) -> coro<Result<Option<Bytes>>> {
    co_return Ok(rstd::move(bytes));
}
auto invalid_window() -> coro<Result<Option<Bytes>>> {
    co_return Err(Error::InvalidOptions(String::make("zero upload window"_str)));
}
auto source_wait_closed(Option<Arc<UploadSourceState>> source) -> coro<CleanupReport> {
    if (source.is_none()) co_return CleanupReport {};
    auto state = source.take().unwrap();
    auto gate  = co_await state->close_gate.lock();
    if (! state->closed) {
        if (state->release.is_none())
            state->release = Some(state->source->release({}).into_future());
        auto result    = co_await rstd::future::by_ref(*state->release);
        state->release = None();
        state->closed  = true;
        state->outcome.complete(rstd::move(result));
    }
    co_return state->outcome.report();
}
auto source_close(Option<Arc<UploadSourceState>> source) -> coro<Result<empty>> {
    if (source.is_none()) co_return Ok(empty {});
    (void)co_await source_wait_closed(source.clone());
    auto result = (*source)->outcome.take_result();
    if (result.is_none()) co_return Err(Error::Contract("upload close result already taken"_Str));
    co_return result.take().unwrap();
}
auto UploadBody::next(usize maximum) -> coro<Result<Option<Bytes>>> {
    if (maximum == usize()) return invalid_window();
    if (source_.is_some()) return source_next((*source_).clone(), maximum);
    if (bytes_.is_none()) return memory_next(None());
    if (bytes_->len() <= maximum) return memory_next(bytes_.take());
    auto bytes = bytes_->slice(usize(), maximum);
    bytes_->advance(maximum);
    return memory_next(Some(rstd::move(bytes)));
}
auto UploadBody::close() const -> coro<Result<rstd::empty>> {
    return source_close(source_.clone());
}
auto UploadBody::wait_closed() const -> coro<CleanupReport> {
    return source_wait_closed(source_.clone());
}
export struct UploadMetadata {
    Option<u64> length { Some(u64()) };
};
export struct UploadFactory {
    template<class Self, class = void>
    struct Api {
        using Trait = UploadFactory;
        auto open(TransferContext context) const -> coro<Result<UploadBody>> {
            return rstd::trait_call<0>(this, rstd::move(context));
        }
        void request_stop(StopReason reason) const { rstd::trait_call<1>(this, reason); }
        auto release(ReleaseContext context) const -> coro<Result<rstd::empty>> {
            return rstd::trait_call<2>(this, context);
        }
    };
    template<class T>
    using Funcs = rstd::TraitFuncs<&T::open, &T::request_stop, &T::release>;
};
struct UploadPlanFields {
    Option<UploadBody> once;
    bool               opened {}, single_use {}, closing {};
};
struct UploadPlanState {
    Option<Bytes>                             bytes;
    Option<Arc<rstd::dyn<UploadFactory>>>     factory;
    UploadMetadata                            metadata;
    rstd::sync::Mutex<UploadPlanFields>       fields { UploadPlanFields {} };
    rstd::async::Mutex<empty>                 close_gate { empty {} };
    bool                                      closed {};
    CloseOutcome                              outcome;
    Option<coro<Result<rstd::empty>>::Future> close_task;
};
export class UploadPlan {
    Arc<UploadPlanState> state_ { Arc<UploadPlanState>::make() };

public:
    UploadPlan()                      = default;
    UploadPlan(UploadPlan&&) noexcept = default;
    UploadPlan(const UploadPlan&)     = delete;
    ~UploadPlan() { request_close(); }
    auto operator=(UploadPlan&& other) noexcept -> UploadPlan& {
        if (this != &other) {
            request_close();
            state_ = rstd::move(other.state_);
        }
        return *this;
    }
    static auto empty() -> UploadPlan { return UploadPlan {}; }
    static auto bytes(Bytes bytes) -> UploadPlan {
        UploadPlan plan;
        plan.state_->metadata.length = Some(u64(bytes.len().to_primitive()));
        plan.state_->bytes           = Some(rstd::move(bytes));
        return plan;
    }
    static auto once(UploadBody body) -> UploadPlan {
        UploadPlan plan;
        plan.state_->metadata.length = body.length();
        {
            auto f        = plan.state_->fields.lock().unwrap();
            f->once       = Some(rstd::move(body));
            f->single_use = true;
        }
        return plan;
    }
    static auto repeatable(UploadMetadata metadata, Arc<rstd::dyn<UploadFactory>> factory)
        -> UploadPlan {
        UploadPlan plan;
        plan.state_->metadata = metadata;
        plan.state_->factory  = Some(rstd::move(factory));
        return plan;
    }
    auto metadata() const -> UploadMetadata { return state_->metadata; }
    auto length() const -> Option<u64> { return state_->metadata.length; }
    auto replayable() const -> bool {
        auto f = state_->fields.lock().unwrap();
        return ! f->single_use || ! f->opened;
    }
    auto open(TransferContext context) const -> coro<Result<UploadBody>>;
    void request_close() const {
        if (! state_) return;
        if (state_->factory.is_some()) (*state_->factory)->request_stop(StopReason::Abandoned);
        auto f     = state_->fields.lock().unwrap();
        f->closing = true;
        if (f->once.is_some()) f->once->cancel();
    }
    auto close() const -> coro<Result<rstd::empty>>;
    auto wait_closed() const -> coro<CleanupReport>;
    auto take_close_result() const -> Option<Result<rstd::empty>> {
        return state_->outcome.take_result();
    }
};
auto open_upload(Arc<UploadPlanState> state, TransferContext context) -> coro<Result<UploadBody>> {
    {
        auto f = state->fields.lock().unwrap();
        if (f->closing) co_return Err(Error::Closed());
    }
    if (state->factory.is_some()) {
        auto body = co_await (*state->factory)->open(context.clone());
        if (body.is_err()) co_return Err(rstd::move(body).unwrap_err());
        if (body->length() != state->metadata.length) {
            auto closed = co_await body->close();
            if (closed.is_err())
                context.resources().record_cleanup_error(rstd::move(closed).unwrap_err());
            co_return Err(
                Error::Contract(String::make("factory upload length differs from metadata"_str)));
        }
        co_return rstd::move(body);
    }
    if (state->bytes.is_some()) co_return Ok(UploadBody::bytes(state->bytes->clone()));
    auto f = state->fields.lock().unwrap();
    if (f->once.is_some()) {
        f->opened = true;
        co_return Ok(f->once.take().unwrap());
    }
    if (f->opened) co_return Err(Error::ReplayUnavailable());
    co_return Ok(UploadBody {});
}
auto UploadPlan::open(TransferContext context) const -> coro<Result<UploadBody>> {
    return open_upload(state_.clone(), rstd::move(context));
}
auto close_upload_sources(Option<UploadBody> once, Option<Arc<rstd::dyn<UploadFactory>>> factory)
    -> coro<Result<rstd::empty>> {
    Option<Error> error;
    if (once.is_some()) {
        once->cancel();
        auto result = co_await once->close();
        if (result.is_err()) error = Some(rstd::move(result).unwrap_err());
    }
    if (factory.is_some()) {
        auto result = co_await (*factory)->release({});
        if (result.is_err() && error.is_none()) error = Some(rstd::move(result).unwrap_err());
    }
    if (error.is_some()) co_return Err(error.take().unwrap());
    co_return Ok(rstd::empty {});
}
auto wait_upload_closed(Arc<UploadPlanState> state) -> coro<CleanupReport> {
    auto gate = co_await state->close_gate.lock();
    if (! state->closed) {
        if (state->close_task.is_none()) {
            Option<UploadBody> once;
            {
                auto f = state->fields.lock().unwrap();
                once   = f->once.take();
            }
            state->close_task =
                Some(close_upload_sources(rstd::move(once), state->factory.clone()).into_future());
        }
        auto result       = co_await rstd::future::by_ref(*state->close_task);
        state->close_task = None();
        state->closed     = true;
        state->outcome.complete(rstd::move(result));
    }
    co_return state->outcome.report();
}
auto close_upload(Arc<UploadPlanState> state) -> coro<Result<rstd::empty>> {
    (void)co_await wait_upload_closed(state.clone());
    auto result = state->outcome.take_result();
    if (result.is_none())
        co_return Err(Error::Contract("upload plan close result already taken"_Str));
    co_return result.take().unwrap();
}
auto UploadPlan::close() const -> coro<Result<rstd::empty>> {
    request_close();
    return close_upload(state_.clone());
}
auto UploadPlan::wait_closed() const -> coro<CleanupReport> {
    request_close();
    return wait_upload_closed(state_.clone());
}
} // namespace lifetchto
export namespace rstd
{
template<class S>
struct Impl<lifetchto::ByteSource, lifetchto::StoredByteSource<S>>
    : ImplBase<lifetchto::StoredByteSource<S>> {
    auto next(usize maximum) const { return this->self().next(maximum); }
    void request_stop(lifetchto::StopReason reason) const { this->self().request_stop(reason); }
    auto release(lifetchto::ReleaseContext context) const { return this->self().release(context); }
};
} // namespace rstd
namespace lifetchto
{
template<class S>
auto UploadBody::from_source(S source, Option<u64> length) -> UploadBody {
    return UploadBody::source(
        Arc<rstd::dyn<ByteSource>>::make(StoredByteSource<S> { Arc<S>::make(rstd::move(source)) }),
        length);
}
} // namespace lifetchto
namespace lifetchto
{
struct ByteQueueFields {
    Vec<Bytes>             queue;
    usize                  buffered {};
    bool                   finished {}, reading {}, failed {};
    Option<Error>          error;
    Option<ResourcePermit> permit;
};
struct ByteQueueState {
    usize                              capacity;
    Cancellation                       cancellation;
    Option<Instant>                    deadline;
    rstd::sync::Mutex<ByteQueueFields> fields { ByteQueueFields {} };
    Event                              changed;
    ByteQueueState(usize capacity, Cancellation cancellation, Option<Instant> deadline)
        : capacity(capacity), cancellation(rstd::move(cancellation)), deadline(deadline) {}
};
export class ByteQueue {
    Arc<ByteQueueState> state_;

public:
    explicit ByteQueue(Arc<ByteQueueState> state): state_(rstd::move(state)) {}
    static auto make(ResourceContext context, usize capacity) -> coro<Result<ByteQueue>>;
    auto        clone() const -> ByteQueue { return ByteQueue(state_.clone()); }
    auto        available() const -> usize {
        auto f = state_->fields.lock().unwrap();
        return state_->capacity - f->buffered;
    }
    auto changed() const -> EventWait { return state_->changed.listen(); }
    auto readable() const -> Result<bool> {
        if (state_->cancellation.is_canceled())
            return Err(stop_error(*state_->cancellation.reason()));
        auto f = state_->fields.lock().unwrap();
        return Ok(! f->queue.is_empty() || f->finished || f->failed);
    }
    auto wait_readable() const -> coro<Result<empty>>;
    auto wait_writable() const -> coro<Result<empty>>;
    auto try_send(Bytes bytes) const -> Result<empty> {
        if (state_->cancellation.is_canceled())
            return Err(stop_error(*state_->cancellation.reason()));
        {
            auto f = state_->fields.lock().unwrap();
            if (f->finished || f->failed) return Err(Error::Closed());
            if (bytes.len() > state_->capacity - f->buffered) return Err(Error::Capacity());
            f->buffered += bytes.len();
            f->queue.push(rstd::move(bytes));
        }
        state_->changed.notify();
        return Ok(empty {});
    }
    auto send(Bytes bytes) const -> coro<Result<empty>>;
    auto next(usize maximum = usize(65536)) const -> coro<Result<Option<Bytes>>>;
    auto try_next(usize maximum = usize(65536)) const -> Result<Option<Option<Bytes>>>;
    void finish() const {
        {
            auto f      = state_->fields.lock().unwrap();
            f->finished = true;
        }
        state_->changed.notify();
    }
    void fail(Error error) const {
        {
            auto f = state_->fields.lock().unwrap();
            if (f->finished && f->buffered == usize()) return;
            if (! f->failed) {
                f->failed = true;
                f->error  = Some(rstd::move(error));
            }
        }
        state_->changed.notify();
    }
    void release() const {
        {
            auto f = state_->fields.lock().unwrap();
            f->queue.clear();
            f->buffered = usize();
            f->permit   = None();
            f->finished = true;
        }
        state_->changed.notify();
    }
};
// Queue waits retain their own state and cancellation registration.
auto queue_changed(Arc<ByteQueueState> state, EventWait event) -> coro<Result<empty>> {
    if (state->cancellation.is_canceled()) co_return Err(stop_error(*state->cancellation.reason()));
    auto canceled = state->cancellation.stopped().into_future();
    if (state->deadline.is_some()) {
        auto waiting = rstd::async::select(rstd::move(event), rstd::move(canceled));
        auto result = co_await rstd::async::select(rstd::move(waiting),
                                                   rstd::async::sleep(remaining(*state->deadline)));
        if (result.is_right()) co_return Err(Error::Timeout());
        auto ready = rstd::move(result).unwrap_left();
        if (ready.is_right()) co_return Err(stop_error(rstd::move(ready).unwrap_right()));
    } else {
        auto ready = co_await rstd::async::select(rstd::move(event), rstd::move(canceled));
        if (ready.is_right()) co_return Err(stop_error(rstd::move(ready).unwrap_right()));
    }
    co_return Ok(empty {});
}
auto wait_queue_state(Arc<ByteQueueState> state, bool writing) -> coro<Result<empty>> {
    for (;;) {
        auto event = state->changed.listen();
        if (state->cancellation.is_canceled())
            co_return Err(stop_error(*state->cancellation.reason()));
        {
            auto f = state->fields.lock().unwrap();
            if (writing && (f->finished || f->failed)) co_return Err(Error::Closed());
            if (writing ? f->buffered < state->capacity
                        : ! f->queue.is_empty() || f->finished || f->failed)
                co_return Ok(empty {});
        }
        auto changed = co_await queue_changed(state.clone(), rstd::move(event));
        if (changed.is_err()) co_return Err(rstd::move(changed).unwrap_err());
    }
}
auto ByteQueue::wait_readable() const -> coro<Result<empty>> {
    return wait_queue_state(state_.clone(), false);
}
auto ByteQueue::wait_writable() const -> coro<Result<empty>> {
    return wait_queue_state(state_.clone(), true);
}
auto ByteQueue::make(ResourceContext context, usize capacity) -> coro<Result<ByteQueue>> {
    if (capacity == usize())
        co_return Err(Error::InvalidOptions(String::make("zero byte queue capacity"_str)));
    auto permit = co_await context.reserve(ResourceAmount { .bytes = capacity });
    if (permit.is_err()) co_return Err(rstd::move(permit).unwrap_err());
    auto state = Arc<ByteQueueState>::make(capacity, context.cancellation(), context.deadline());
    state->fields.lock().unwrap()->permit = Some(rstd::move(permit).unwrap());
    co_return Ok(ByteQueue(rstd::move(state)));
}
struct QueueReadGuard {
    Arc<ByteQueueState> state;
    bool                complete {};
    ~QueueReadGuard() {
        {
            auto f     = state->fields.lock().unwrap();
            f->reading = false;
            if (! complete && ! f->failed) {
                f->failed = true;
                f->error  = Some(Error::Canceled());
            }
        }
        state->changed.notify();
    }
};
auto take_queued(const Arc<ByteQueueState>& state, usize maximum) -> Result<Option<Option<Bytes>>> {
    auto f = state->fields.lock().unwrap();
    if (f->error.is_some()) return Err(f->error.take().unwrap());
    if (f->failed) return Err(Error::Closed());
    if (state->deadline.is_some() && Instant::now() >= *state->deadline)
        return Err(Error::Timeout());
    if (state->cancellation.is_canceled()) return Err(stop_error(*state->cancellation.reason()));
    if (! f->queue.is_empty()) {
        auto bytes = f->queue[usize()].clone();
        if (bytes.len() > maximum) {
            bytes = bytes.slice(usize(), maximum);
            f->queue[usize()].advance(maximum);
        } else
            f->queue.remove(usize());
        f->buffered -= bytes.len();
        state->changed.notify();
        return Ok(Some(Some(rstd::move(bytes))));
    }
    if (f->finished) return Ok(Some(None<Bytes>()));
    return Ok(None<Option<Bytes>>());
}
auto ByteQueue::try_next(usize maximum) const -> Result<Option<Option<Bytes>>> {
    if (maximum == usize()) return Err(Error::InvalidOptions("zero read window"_Str));
    {
        auto f = state_->fields.lock().unwrap();
        if (f->reading) return Err(Error::Busy());
        f->reading = true;
    }
    QueueReadGuard guard { state_.clone(), true };
    return take_queued(state_, maximum);
}
auto queue_next(Arc<ByteQueueState> state, usize maximum) -> coro<Result<Option<Bytes>>> {
    if (maximum == usize()) co_return Err(Error::InvalidOptions("zero read window"_Str));
    {
        auto f = state->fields.lock().unwrap();
        if (f->reading) co_return Err(Error::Busy());
        f->reading = true;
    }
    QueueReadGuard guard { state.clone() };
    for (;;) {
        auto event = state->changed.listen();
        auto ready = take_queued(state, maximum);
        if (ready.is_err()) {
            guard.complete = true;
            co_return Err(rstd::move(ready).unwrap_err());
        }
        if (ready->is_some()) {
            guard.complete = true;
            co_return Ok(ready->take().unwrap());
        }
        auto changed = co_await queue_changed(state.clone(), rstd::move(event));
        if (changed.is_err()) {
            guard.complete = true;
            co_return Err(rstd::move(changed).unwrap_err());
        }
    }
}
auto queue_send(Arc<ByteQueueState> state, Bytes bytes) -> coro<Result<empty>> {
    if (bytes.len() > state->capacity) co_return Err(Error::TooLarge());
    for (;;) {
        auto event = state->changed.listen();
        auto sent  = ByteQueue(state.clone()).try_send(bytes.clone());
        if (sent.is_ok()) co_return Ok(empty {});
        if (! sent.as_ref().unwrap_err().is_Capacity())
            co_return Err(rstd::move(sent).unwrap_err());
        auto ready = co_await queue_changed(state.clone(), rstd::move(event));
        if (ready.is_err()) co_return Err(rstd::move(ready).unwrap_err());
    }
}
auto ByteQueue::next(usize maximum) const -> coro<Result<Option<Bytes>>> {
    return queue_next(state_.clone(), maximum);
}
auto ByteQueue::send(Bytes bytes) const -> coro<Result<empty>> {
    return queue_send(state_.clone(), rstd::move(bytes));
}
} // namespace lifetchto
