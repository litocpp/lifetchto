module lifetchto.core;
import :engine;
using namespace rstd::prelude;
using namespace rstd::literals;

namespace lifetchto
{
namespace
{
rstd::sync::Mutex<u64> cleanup_sequence { u64(1) };
} // namespace

auto Cancellation::wait(Arc<CancelState> state) -> coro<StopReason> {
    for (;;) {
        auto event = state->changed.listen();
        {
            auto f = state->fields.lock().unwrap();
            if (f->reason.is_some()) co_return *f->reason;
        }
        co_await rstd::move(event);
    }
}

void Cancellation::cancel(StopReason reason) const {
    {
        auto f = state_->fields.lock().unwrap();
        if (f->reason.is_none()) f->reason = Some(reason);
    }
    state_->changed.notify();
}

auto Cancellation::reason() const -> Option<StopReason> {
    auto f = state_->fields.lock().unwrap();
    return f->reason;
}

void CleanupState::note(u64 identity, FailureReport diagnostic) const {
    {
        auto f = fields.lock().unwrap();
        for (auto seen : f->seen)
            if (seen == identity) return;
        f->seen.push(u64(identity));
        f->report.add(diagnostic.clone());
    }
    if (parent) (*parent)->note(identity, rstd::move(diagnostic));
}

void CleanupState::deliver(Error error) const {
    {
        auto f = fields.lock().unwrap();
        if (! f->forwarded) {
            f->pending.push(rstd::move(error));
            return;
        }
    }
    if (parent) (*parent)->deliver(rstd::move(error));
}

void CleanupState::record(Error error) const {
    if (error.is_Multiple()) {
        for (auto& item : error.as_Multiple().errors) record(rstd::move(item));
        return;
    }
    if (error.cleanup_id_.is_none()) {
        auto next         = cleanup_sequence.lock().unwrap();
        error.cleanup_id_ = Some((*next)++);
    }
    note(*error.cleanup_id_, report_failure(error));
    deliver(rstd::move(error));
}

auto CleanupState::report() const -> CleanupReport {
    return fields.lock().unwrap()->report.clone();
}

auto CleanupState::take() const -> Option<Vec<Error>> {
    auto f = fields.lock().unwrap();
    if (f->forwarded) return None();
    return Some(rstd::exchange(f->pending, Vec<Error> {}));
}

void CleanupState::forward() const {
    Vec<Error> pending;
    {
        auto f = fields.lock().unwrap();
        if (f->forwarded) return;
        f->forwarded = true;
        pending      = rstd::move(f->pending);
    }
    if (parent)
        for (auto& error : pending) (*parent)->deliver(rstd::move(error));
}

void ResourcePermit::reset() {
    if (budget_.is_none()) return;
    auto state = budget_.take().unwrap();
    {
        auto f = state->fields.lock().unwrap();
        f->used.operations -= amount_.operations;
        f->used.tasks -= amount_.tasks;
        f->used.resources -= amount_.resources;
        f->used.bytes -= amount_.bytes;
    }
    state->changed.notify();
}

void TransferScope::request_close() const {
    if (scope_) {
        seal_scope(scope_, true);
        if (scope_->parent_record.is_some()) {
            auto record = scope_->parent_record->upgrade();
            if (record) start_release(record);
        }
    }
}

auto TransferScope::close() const -> coro<CleanupReport> {
    request_close();
    return drain_scope(scope_.clone());
}

auto TransferScope::take_close_errors() -> Vec<Error> {
    return scope_->cleanup->take().unwrap_or(Vec<Error> {});
}

auto Engine::make(rstd::async::RuntimeHandle runtime, EngineOptions options) -> Result<Engine> {
    if (options.limits.operations == usize() || options.limits.tasks == usize() ||
        options.limits.resources == usize() || options.limits.bytes == usize())
        return Err(Error::InvalidOptions(String::make("zero engine capacity"_str)));
    return Ok(Engine(TransferScope(
        Arc<ScopeState>::make(rstd::move(runtime), Arc<BudgetState>::make(options)))));
}

auto stop_error(StopReason reason) -> Error {
    return reason == StopReason::Deadline ? Error::Timeout() : Error::Canceled();
}

auto remaining(Instant deadline) -> Duration {
    auto now = Instant::now();
    return deadline > now ? deadline - now : Duration {};
}

auto try_reserve(Arc<BudgetState> budget, ResourceAmount amount) -> Result<ResourcePermit> {
    auto        f     = budget->fields.lock().unwrap();
    const auto& limit = budget->options.limits;
    if (amount.operations > limit.operations - f->used.operations ||
        amount.tasks > limit.tasks - f->used.tasks ||
        amount.resources > limit.resources - f->used.resources ||
        amount.bytes > limit.bytes - f->used.bytes)
        return Err(Error::Capacity());
    f->used.operations += amount.operations;
    f->used.tasks += amount.tasks;
    f->used.resources += amount.resources;
    f->used.bytes += amount.bytes;
    return Ok(ResourcePermit(budget.clone(), amount));
}

auto release_resource(Arc<ResourceRecord> record) -> coro<empty> {
    auto result = co_await record->owner->release(ReleaseContext {});
    {
        auto f = record->flags.lock().unwrap();
        if (result.is_err()) record->cleanup->record(rstd::move(result).unwrap_err());
        *record->observation->report.lock().unwrap() = Some(record->cleanup->report());
        record->permit                               = None();
        f->done                                      = true;
    }
    record->changed.notify();
    co_return empty {};
}

void start_release(const Arc<ResourceRecord>& record) {
    {
        auto f = record->flags.lock().unwrap();
        if (f->started) return;
        f->started = true;
    }
    auto task = record->runtime.spawn(release_resource(record.clone()));
    {
        auto f       = record->flags.lock().unwrap();
        record->task = Some(rstd::move(task));
        f->ready     = true;
    }
    record->changed.notify();
}

auto join_resource(Arc<ResourceRecord> record) -> coro<empty> {
    start_release(record);
    for (;;) {
        auto event = record->changed.listen();
        {
            auto f = record->flags.lock().unwrap();
            if (f->ready) break;
        }
        co_await rstd::move(event);
    }
    auto gate = co_await record->join_gate.lock();
    if (record->task.is_some()) {
        auto joined  = co_await rstd::future::by_ref(*record->task);
        auto f       = record->flags.lock().unwrap();
        record->task = None();
        f->joined    = true;
        if (joined.is_err() && ! f->done) {
            record->cleanup->record(Error::Closed());
            *record->observation->report.lock().unwrap() = Some(record->cleanup->report());
        }
    }
    co_return empty {};
}

auto take_resource_result(const Arc<ResourceRecord>& record) -> Option<Result<empty>> {
    auto f = record->flags.lock().unwrap();
    if (! f->joined || f->result_taken) return None();
    auto errors = record->cleanup->take();
    if (errors.is_none()) return None();
    f->result_taken = true;
    return Some(collect_errors(errors.take().unwrap()));
}

auto wait_resource(Arc<ResourceRecord> record) -> coro<Result<empty>> {
    co_await join_resource(record.clone());
    auto result = take_resource_result(record);
    if (result.is_none())
        co_return Err(Error::Contract("release result already taken or transferred"_Str));
    co_return result.take().unwrap();
}

auto wait_resource_report(Arc<ResourceRecord> record) -> coro<CleanupReport> {
    co_await join_resource(record.clone());
    co_return record->cleanup->report();
}

auto wait_unregistered_close(Arc<UnregisteredClose> state) -> coro<CleanupReport> {
    auto gate = co_await state->gate.lock();
    if (! state->closed) {
        if (state->task.is_none()) state->task = Some(state->owner->release({}).into_future());
        auto result   = co_await rstd::future::by_ref(*state->task);
        state->task   = None();
        state->closed = true;
        state->outcome.complete(rstd::move(result));
    }
    co_return state->outcome.report();
}

auto release_unregistered(Arc<UnregisteredClose> state) -> coro<Result<empty>> {
    (void)co_await wait_unregistered_close(state.clone());
    auto result = state->outcome.take_result();
    if (result.is_none()) co_return Err(Error::Contract("close result already taken"_Str));
    co_return result.take().unwrap();
}

void seal_scope(const Arc<ScopeState>& scope, bool stop, StopReason reason) {
    Vec<Arc<ResourceRecord>> entries;
    {
        auto f    = scope->fields.lock().unwrap();
        f->closed = true;
        for (const auto& record : f->entries) entries.push(record.clone());
    }
    if (stop) scope->cancellation.cancel(reason);
    for (auto& record : entries) {
        if (stop) record->stop(reason);
        start_release(record);
    }
}

auto drain_scope(Arc<ScopeState> scope) -> coro<CleanupReport> {
    Vec<Arc<ResourceRecord>> entries;
    {
        auto f = scope->fields.lock().unwrap();
        for (const auto& record : f->entries) entries.push(record.clone());
    }
    for (auto& record : entries) co_await join_resource(record.clone());
    {
        auto f = scope->fields.lock().unwrap();
        f->entries.clear();
    }
    entries.clear();
    co_return scope->cleanup->report();
}

auto admit_resource(const Arc<ScopeState>& scope, Arc<ResourceRecord> record) -> Result<empty> {
    auto f = scope->fields.lock().unwrap();
    if (f->closed || scope->cancellation.is_canceled()) return Err(Error::Closed());
    for (usize i; i < f->entries.len();) {
        if (f->entries[i]->finished()) {
            f->entries.remove(i);
        } else
            ++i;
    }
    f->entries.push(rstd::move(record));
    return Ok(empty {});
}

auto observe_resource(rstd::sync::Weak<ResourceRecord> record, Arc<ResourceObservation> observation)
    -> coro<CleanupReport> {
    auto owner = record.upgrade();
    if (owner) co_await join_resource(rstd::move(owner));
    auto report = observation->report.lock().unwrap();
    if (report->is_some()) co_return (**report).clone();
    CleanupReport unavailable;
    unavailable.total_errors = usize(1);
    unavailable.errors.push(report_failure(Error::Closed()));
    co_return rstd::move(unavailable);
}

auto reserve_resource(ResourceContext  context,
                      Arc<BudgetState> budget,
                      ResourceAmount   amount,
                      Option<Instant>  deadline) -> coro<Result<ResourcePermit>> {
    auto        cancellation = context.cancellation();
    const auto& limits       = budget->options.limits;
    if (amount.operations > limits.operations || amount.tasks > limits.tasks ||
        amount.resources > limits.resources || amount.bytes > limits.bytes)
        co_return Err(Error::Capacity());
    for (;;) {
        auto event = budget->changed.listen();
        if (context.closed() || cancellation.is_canceled()) co_return Err(Error::Closed());
        if (deadline.is_some() && Instant::now() >= *deadline) co_return Err(Error::Timeout());
        auto permit = try_reserve(budget.clone(), amount);
        if (permit.is_ok()) co_return rstd::move(permit);
        auto stopped = cancellation.stopped().into_future();
        if (deadline.is_some()) {
            auto waiting = rstd::async::select(rstd::move(event), rstd::move(stopped));
            auto ready   = co_await rstd::async::select(rstd::move(waiting),
                                                        rstd::async::sleep(remaining(*deadline)));
            if (ready.is_right()) co_return Err(Error::Timeout());
        } else
            (void)co_await rstd::async::select(rstd::move(event), rstd::move(stopped));
    }
}

auto ResourceContext::reserve(ResourceAmount amount) const -> coro<Result<ResourcePermit>> {
    return reserve_resource(clone(), scope_->budget.clone(), amount, deadline_);
}

void stop_operation(const Arc<OperationState>& state, StopReason reason) {
    {
        auto f = state->fields.lock().unwrap();
        if (f->phase == TransferPhase::Released) return;
    }
    seal_scope(state->scope, true, reason);
    state->changed.notify();
}

auto wait_transfer(Arc<OperationState> state) -> coro<Arc<TransferReport>> {
    if (state->record.is_some()) {
        auto record = state->record->upgrade();
        if (record) (void)co_await wait_resource(rstd::move(record));
    }
    for (;;) {
        auto event = state->changed.listen();
        {
            auto f = state->fields.lock().unwrap();
            if (f->report.is_some()) co_return f->report->clone();
        }
        co_await rstd::move(event);
    }
}

auto TransferControl::wait() const -> coro<Arc<TransferReport>> {
    return wait_transfer(state_.clone());
}

auto context_delay(TransferContext context, Duration duration) -> coro<Result<empty>> {
    auto until    = Instant::now() + duration;
    auto deadline = context.deadline();
    if (deadline.is_some() && *deadline < until) until = *deadline;
    auto cancellation = context.cancellation();
    if (cancellation.is_canceled()) co_return Err(stop_error(*cancellation.reason()));
    auto result = co_await rstd::async::select(rstd::async::sleep(remaining(until)),
                                               cancellation.stopped().into_future());
    if (result.is_right()) co_return Err(stop_error(rstd::move(result).unwrap_right()));
    if (deadline.is_some() && Instant::now() >= *deadline) co_return Err(Error::Timeout());
    co_return Ok(empty {});
}

auto TransferContext::delay(Duration duration) const -> coro<Result<empty>> {
    return context_delay(clone(), duration);
}

auto ResourceContext::handle() const -> EngineHandle {
    return EngineHandle(scope_.downgrade());
}

auto TransferScope::handle() const -> EngineHandle {
    return EngineHandle(scope_.downgrade());
}

struct ScopeResource {
    Arc<ScopeState> scope;
    void            request_stop(StopReason reason) const { seal_scope(scope, true, reason); }
    auto            release(ReleaseContext) const -> coro<Result<empty>> {
        seal_scope(scope, false);
        (void)co_await drain_scope(scope.clone());
        co_return Ok(empty {});
    }
};
} // namespace lifetchto

namespace rstd
{
template<>
struct Impl<lifetchto::ManagedResource, lifetchto::ScopeResource>
    : ImplBase<lifetchto::ScopeResource> {
    void request_stop(lifetchto::StopReason reason) const { this->self().request_stop(reason); }
    auto release(lifetchto::ReleaseContext context) const { return this->self().release(context); }
};
} // namespace rstd

namespace lifetchto
{
auto EngineHandle::scope(ScopeOptions) const -> Result<TransferScope> {
    auto parent = scope_.upgrade();
    if (! parent) return Err(Error::Closed());
    auto permit =
        lifetchto::try_reserve(parent->budget.clone(), ResourceAmount { .resources = usize(1) });
    if (permit.is_err()) return Err(rstd::move(permit).unwrap_err());
    auto scope = Arc<ScopeState>::make(
        parent->runtime.clone(), parent->budget.clone(), Some(parent->cleanup.clone()));
    auto owner  = Arc<rstd::dyn<ManagedResource>>::make(ScopeResource { scope.clone() });
    auto record = Arc<ResourceRecord>::make(
        rstd::move(owner), parent->runtime.clone(), parent->cleanup.clone());
    record->permit       = Some(rstd::move(permit).unwrap());
    scope->parent_record = Some(record.downgrade());
    auto admitted        = admit_resource(parent, record.clone());
    if (admitted.is_err()) return Err(rstd::move(admitted).unwrap_err());
    return Ok(TransferScope(rstd::move(scope)));
}
} // namespace lifetchto
