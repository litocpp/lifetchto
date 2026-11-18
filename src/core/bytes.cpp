module lifetchto.core;
import :bytes;
using namespace rstd::prelude;
using namespace rstd::literals;

namespace lifetchto
{
struct UploadSourceState {
    Arc<rstd::dyn<ByteSource>>          source;
    rstd::async::Mutex<empty>           close_gate { empty {} };
    Option<coro<Result<empty>>::Future> release;
    CloseOutcome                        outcome;
    bool                                closed {};
    explicit UploadSourceState(Arc<rstd::dyn<ByteSource>> source): source(rstd::move(source)) {}
};

UploadBody::UploadBody() = default;
UploadBody::UploadBody(UploadBody&& other) noexcept
    : bytes_(other.bytes_.take()), source_(other.source_.take()), length_(other.length_) {
}
auto UploadBody::operator=(UploadBody&& other) noexcept -> UploadBody& {
    if (this != &other) {
        cancel();
        bytes_  = rstd::move(other.bytes_);
        source_ = other.source_.take();
        length_ = other.length_;
    }
    return *this;
}
UploadBody::~UploadBody() {
    cancel();
}

auto UploadBody::bytes(Bytes bytes) -> UploadBody {
    UploadBody result;
    result.length_ = Some(u64(bytes.len().to_primitive()));
    result.bytes_  = Some(rstd::move(bytes));
    return result;
}

auto UploadBody::source(Arc<rstd::dyn<ByteSource>> source, Option<u64> length) -> UploadBody {
    UploadBody result;
    result.source_ = Some(Arc<UploadSourceState>::make(rstd::move(source)));
    result.length_ = length;
    return result;
}

void UploadBody::cancel() const {
    if (source_.is_some()) (*source_)->source->request_stop(StopReason::Abandoned);
}

auto UploadBody::take_close_result() const -> Option<Result<empty>> {
    return source_.is_some() ? (*source_)->outcome.take_result()
                             : Some(Result<empty>(Ok(empty {})));
}

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

UploadPlan::UploadPlan(): state_(Arc<UploadPlanState>::make()) {
}
UploadPlan::UploadPlan(UploadPlan&&) noexcept = default;
UploadPlan::~UploadPlan() {
    request_close();
}
auto UploadPlan::operator=(UploadPlan&& other) noexcept -> UploadPlan& {
    if (this != &other) {
        request_close();
        state_ = rstd::move(other.state_);
    }
    return *this;
}

auto UploadPlan::bytes(Bytes bytes) -> UploadPlan {
    UploadPlan plan;
    plan.state_->metadata.length = Some(u64(bytes.len().to_primitive()));
    plan.state_->bytes           = Some(rstd::move(bytes));
    return plan;
}

auto UploadPlan::once(UploadBody body) -> UploadPlan {
    UploadPlan plan;
    plan.state_->metadata.length = body.length();
    {
        auto f        = plan.state_->fields.lock().unwrap();
        f->once       = Some(rstd::move(body));
        f->single_use = true;
    }
    return plan;
}

auto UploadPlan::repeatable(UploadMetadata metadata, Arc<rstd::dyn<UploadFactory>> factory)
    -> UploadPlan {
    UploadPlan plan;
    plan.state_->metadata = metadata;
    plan.state_->factory  = Some(rstd::move(factory));
    return plan;
}

auto UploadPlan::metadata() const -> UploadMetadata {
    return state_->metadata;
}

auto UploadPlan::length() const -> Option<u64> {
    return state_->metadata.length;
}

auto UploadPlan::replayable() const -> bool {
    auto f = state_->fields.lock().unwrap();
    return ! f->single_use || ! f->opened;
}

void UploadPlan::request_close() const {
    if (! state_) return;
    if (state_->factory.is_some()) (*state_->factory)->request_stop(StopReason::Abandoned);
    auto f     = state_->fields.lock().unwrap();
    f->closing = true;
    if (f->once.is_some()) f->once->cancel();
}

auto UploadPlan::take_close_result() const -> Option<Result<rstd::empty>> {
    return state_->outcome.take_result();
}

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

ByteQueue::ByteQueue(Arc<ByteQueueState> state): state_(rstd::move(state)) {
}
ByteQueue::ByteQueue(ByteQueue&&) noexcept                    = default;
ByteQueue::~ByteQueue()                                       = default;
auto ByteQueue::operator=(ByteQueue&&) noexcept -> ByteQueue& = default;

auto ByteQueue::clone() const -> ByteQueue {
    return ByteQueue(state_.clone());
}

auto ByteQueue::available() const -> usize {
    auto f = state_->fields.lock().unwrap();
    return state_->capacity - f->buffered;
}

auto ByteQueue::changed() const -> EventWait {
    return state_->changed.listen();
}

auto ByteQueue::readable() const -> Result<bool> {
    if (state_->cancellation.is_canceled()) return Err(stop_error(*state_->cancellation.reason()));
    auto f = state_->fields.lock().unwrap();
    return Ok(! f->queue.is_empty() || f->finished || f->failed);
}

auto ByteQueue::try_send(Bytes bytes) const -> Result<empty> {
    if (state_->cancellation.is_canceled()) return Err(stop_error(*state_->cancellation.reason()));
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

void ByteQueue::finish() const {
    {
        auto f      = state_->fields.lock().unwrap();
        f->finished = true;
    }
    state_->changed.notify();
}

void ByteQueue::fail(Error error) const {
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

void ByteQueue::release() const {
    {
        auto f = state_->fields.lock().unwrap();
        f->queue.clear();
        f->buffered = usize();
        f->permit   = None();
        f->finished = true;
    }
    state_->changed.notify();
}

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
