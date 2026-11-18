export module lifetchto.core:engine;
export import :error;
export import :event;
using namespace rstd::prelude;
using namespace rstd::literals;
namespace lifetchto
{
export enum class StopReason { Canceled, Deadline, OwnerClosed, Abandoned };
export enum class RemoteOutcome { NotApplicable, NotDispatched, Unknown, Confirmed };
export enum class TransferPhase { Queued, Running, LogicallyFinished, Releasing, Released };
struct CancelFields {
    Option<StopReason> reason;
};
struct CancelState {
    rstd::sync::Mutex<CancelFields> fields { CancelFields {} };
    Event                           changed;
};
export class Cancellation {
    Arc<CancelState> state_ { Arc<CancelState>::make() };
    static auto      wait(Arc<CancelState> state) -> coro<StopReason>;

public:
    auto clone() const -> Cancellation {
        Cancellation c;
        c.state_ = state_.clone();
        return c;
    }
    void cancel(StopReason reason = StopReason::Canceled) const;
    auto reason() const -> Option<StopReason>;
    auto is_canceled() const -> bool { return reason().is_some(); }
    auto stopped() const -> coro<StopReason> { return wait(state_.clone()); }
};
export auto stop_error(StopReason reason) -> Error;
export auto remaining(Instant deadline) -> Duration;
export struct CleanupReport {
    Vec<FailureReport> errors;
    usize              total_errors {};
    auto               is_ok() const -> bool { return total_errors == usize(); }
    auto               is_err() const -> bool { return ! is_ok(); }
    auto               clone() const -> CleanupReport {
        CleanupReport report;
        report.total_errors = total_errors;
        for (const auto& error : errors) report.errors.push(error.clone());
        return report;
    }
    void add(FailureReport error) {
        ++total_errors;
        if (errors.len() < usize(16)) errors.push(rstd::move(error));
    }
    void merge(CleanupReport report) {
        total_errors += report.total_errors;
        for (auto& error : report.errors)
            if (errors.len() < usize(16)) errors.push(rstd::move(error));
    }
};
struct CleanupFields {
    Vec<Error>    pending;
    Vec<u64>      seen;
    CleanupReport report;
    bool          forwarded {};
};
struct CleanupState {
    Option<Arc<CleanupState>>        parent;
    rstd::sync::Mutex<CleanupFields> fields { CleanupFields {} };
    explicit CleanupState(Option<Arc<CleanupState>> parent = {}): parent(rstd::move(parent)) {}
    void note(u64 identity, FailureReport diagnostic) const;
    void deliver(Error error) const;
    void record(Error error) const;
    auto report() const -> CleanupReport;
    auto take() const -> Option<Vec<Error>>;
    void forward() const;
};
struct CloseOutcomeFields {
    Option<Result<empty>> result;
    CleanupReport         report;
};
struct CloseOutcome {
    rstd::sync::Mutex<CloseOutcomeFields> fields { CloseOutcomeFields {} };
    void                                  complete(Result<empty> result) {
        auto f = fields.lock().unwrap();
        if (result.is_err()) {
            f->report.total_errors = usize(1);
            f->report.errors.push(report_failure(result.as_ref().unwrap_err()));
        }
        f->result = Some(rstd::move(result));
    }
    auto report() const -> CleanupReport { return fields.lock().unwrap()->report.clone(); }
    auto take_result() const -> Option<Result<empty>> {
        return fields.lock().unwrap()->result.take();
    }
};
export struct ResourceAmount {
    usize operations {}, tasks {}, resources {}, bytes {};
};
export struct EngineOptions {
    ResourceAmount limits { usize(256), usize(256), usize(128), usize(64 * 1024 * 1024) };
};
struct BudgetFields {
    ResourceAmount used;
};
struct BudgetState {
    EngineOptions                   options;
    rstd::sync::Mutex<BudgetFields> fields { BudgetFields {} };
    Event                           changed;
    explicit BudgetState(EngineOptions options): options(options) {}
};
export class ResourcePermit {
    Option<Arc<BudgetState>> budget_;
    ResourceAmount           amount_;

public:
    ResourcePermit() = default;
    ResourcePermit(Arc<BudgetState> budget, ResourceAmount amount)
        : budget_(Some(rstd::move(budget))), amount_(amount) {}
    ResourcePermit(ResourcePermit&& other) noexcept
        : budget_(other.budget_.take()), amount_(other.amount_) {}
    ResourcePermit(const ResourcePermit&) = delete;
    auto operator=(ResourcePermit&& other) noexcept -> ResourcePermit& {
        if (this != &other) {
            reset();
            budget_ = other.budget_.take();
            amount_ = other.amount_;
        }
        return *this;
    }
    ~ResourcePermit() { reset(); }
    void reset();
};
auto try_reserve(Arc<BudgetState> budget, ResourceAmount amount) -> Result<ResourcePermit>;
export struct ReleaseContext {
    Option<Instant> graceful_deadline;
};
export struct ManagedResource {
    template<class Self, class = void>
    struct Api {
        using Trait = ManagedResource;
        void request_stop(StopReason reason) const { rstd::trait_call<0>(this, reason); }
        auto release(ReleaseContext context) const -> coro<Result<empty>> {
            return rstd::trait_call<1>(this, context);
        }
    };
    template<class T>
    using Funcs = rstd::TraitFuncs<&T::request_stop, &T::release>;
};
struct ResourceFlags {
    bool started {}, ready {}, done {}, joined {}, result_taken {};
};
struct ResourceObservation {
    rstd::sync::Mutex<Option<CleanupReport>> report { None<CleanupReport>() };
};
struct ResourceRecord {
    Arc<rstd::dyn<ManagedResource>>        owner;
    rstd::async::RuntimeHandle             runtime;
    rstd::sync::Mutex<ResourceFlags>       flags { ResourceFlags {} };
    Event                                  changed;
    rstd::async::Mutex<empty>              join_gate { empty {} };
    Option<rstd::async::JoinHandle<empty>> task;
    Option<ResourcePermit>                 permit;
    Arc<ResourceObservation>               observation { Arc<ResourceObservation>::make() };
    Arc<CleanupState>                      cleanup;
    ResourceRecord(Arc<rstd::dyn<ManagedResource>> owner,
                   rstd::async::RuntimeHandle      runtime,
                   Arc<CleanupState>               parent,
                   bool                            leased = false)
        : owner(rstd::move(owner)),
          runtime(rstd::move(runtime)),
          cleanup(Arc<CleanupState>::make(Some(rstd::move(parent)))) {
        if (! leased) cleanup->forward();
    }
    void stop(StopReason reason) {
        {
            auto f = flags.lock().unwrap();
            if (f->done) return;
        }
        owner->request_stop(reason);
    }
    auto finished() const -> bool {
        auto f = flags.lock().unwrap();
        return f->joined || (f->ready && f->done && task.is_some() && task->is_finished());
    }
};
void start_release(const Arc<ResourceRecord>& record);
auto take_resource_result(const Arc<ResourceRecord>& record) -> Option<Result<empty>>;
auto wait_resource(Arc<ResourceRecord> record) -> coro<Result<empty>>;
auto wait_resource_report(Arc<ResourceRecord> record) -> coro<CleanupReport>;
struct UnregisteredClose {
    Arc<rstd::dyn<ManagedResource>>     owner;
    rstd::async::Mutex<empty>           gate { empty {} };
    Option<coro<Result<empty>>::Future> task;
    CloseOutcome                        outcome;
    bool                                closed {};
    explicit UnregisteredClose(Arc<rstd::dyn<ManagedResource>> owner): owner(rstd::move(owner)) {}
};
auto wait_unregistered_close(Arc<UnregisteredClose> state) -> coro<CleanupReport>;
auto release_unregistered(Arc<UnregisteredClose> state) -> coro<Result<empty>>;
export template<class T>
struct ResourceRegistration {
    T                               access;
    Arc<rstd::dyn<ManagedResource>> owner;

private:
    Arc<UnregisteredClose> cleanup_;

public:
    ResourceRegistration(T access, Arc<rstd::dyn<ManagedResource>> owner)
        : access(rstd::move(access)),
          owner(rstd::move(owner)),
          cleanup_(Arc<UnregisteredClose>::make(this->owner.clone())) {}
    void request_close() const { owner->request_stop(StopReason::Abandoned); }
    auto wait_closed() const -> coro<CleanupReport> {
        request_close();
        return wait_unregistered_close(cleanup_.clone());
    }
    auto take_close_result() -> Option<Result<empty>> { return cleanup_->outcome.take_result(); }
    auto close() const -> coro<Result<empty>> {
        request_close();
        return release_unregistered(cleanup_.clone());
    }
};
export template<class T>
class ResourceLease {
    T                   access_;
    Arc<ResourceRecord> record_;

public:
    ResourceLease(T access, Arc<ResourceRecord> record)
        : access_(rstd::move(access)), record_(rstd::move(record)) {}
    ResourceLease(ResourceLease&&) noexcept = default;
    ResourceLease(const ResourceLease&)     = delete;
    auto operator=(ResourceLease&& other) noexcept -> ResourceLease& {
        if (this != &other) {
            request_stop();
            if (record_) {
                record_->cleanup->forward();
                start_release(record_);
            }
            access_ = rstd::move(other.access_);
            record_ = rstd::move(other.record_);
        }
        return *this;
    }
    ~ResourceLease() {
        if (record_) {
            request_stop();
            record_->cleanup->forward();
            start_release(record_);
        }
    }
    auto get() -> T& { return access_; }
    auto get() const -> const T& { return access_; }
    auto is_released() const -> bool { return record_->finished(); }
    auto into_scope() && -> T {
        auto registration = rstd::move(record_);
        registration->cleanup->forward();
        return rstd::move(access_);
    }
    void request_stop() const {
        if (record_) record_->stop(StopReason::Abandoned);
    }
    auto release() const -> coro<Result<empty>> {
        start_release(record_);
        return wait_resource(record_.clone());
    }
    auto wait_released() const -> coro<CleanupReport> {
        return wait_resource_report(record_.clone());
    }
    auto take_release_result() const -> Option<Result<empty>> {
        return take_resource_result(record_);
    }
};
struct ScopeFields {
    Vec<Arc<ResourceRecord>> entries;
    bool                     closed {};
};
struct ScopeState {
    rstd::async::RuntimeHandle               runtime;
    Arc<BudgetState>                         budget;
    Option<rstd::sync::Weak<ResourceRecord>> parent_record;
    Cancellation                             cancellation;
    rstd::sync::Mutex<ScopeFields>           fields { ScopeFields {} };
    Arc<CleanupState>                        cleanup;
    ScopeState(rstd::async::RuntimeHandle runtime,
               Arc<BudgetState>           budget,
               Option<Arc<CleanupState>>  parent = {})
        : runtime(rstd::move(runtime)),
          budget(rstd::move(budget)),
          cleanup(Arc<CleanupState>::make(rstd::move(parent))) {}
};
void seal_scope(const Arc<ScopeState>& scope,
                bool                   stop,
                StopReason             reason = StopReason::OwnerClosed);
auto drain_scope(Arc<ScopeState> scope) -> coro<CleanupReport>;
auto admit_resource(const Arc<ScopeState>& scope, Arc<ResourceRecord> record) -> Result<empty>;
auto observe_resource(rstd::sync::Weak<ResourceRecord> record, Arc<ResourceObservation> observation)
    -> coro<CleanupReport>;
export class TaskControl {
    rstd::sync::Weak<ResourceRecord> record_;
    Arc<ResourceObservation>         observation_;
    TaskControl(rstd::sync::Weak<ResourceRecord> record, Arc<ResourceObservation> observation)
        : record_(rstd::move(record)), observation_(rstd::move(observation)) {}

public:
    explicit TaskControl(const Arc<ResourceRecord>& record)
        : record_(record.downgrade()), observation_(record->observation.clone()) {}
    auto clone() const -> TaskControl { return TaskControl(record_.clone(), observation_.clone()); }
    void cancel() const {
        auto record = record_.upgrade();
        if (record) record->stop(StopReason::Canceled);
    }
    auto is_finished() const -> bool {
        auto record = record_.upgrade();
        return ! record || record->finished();
    }
    auto wait() const -> coro<CleanupReport> {
        return observe_resource(record_.clone(), observation_.clone());
    }
};
template<class D>
struct TaskState {
    rstd::sync::Mutex<Option<Arc<D>>>              work;
    Cancellation                                   cancellation;
    Option<rstd::async::JoinHandle<Result<empty>>> task;
    explicit TaskState(D work): work(Some(Arc<D>::make(rstd::move(work)))) {}
};
template<class D>
struct TaskResource {
    Arc<TaskState<D>> state;
    void              request_stop(StopReason reason) const {
        state->cancellation.cancel(reason);
        Option<Arc<D>> work;
        {
            auto f = state->work.lock().unwrap();
            if (f->is_some()) work = Some((*f)->clone());
        }
        if constexpr (requires(D& value) { value.request_stop(reason); })
            if (work.is_some()) (*work)->request_stop(reason);
    }
    auto release(ReleaseContext) const -> coro<Result<empty>> {
        auto joined = co_await rstd::future::by_ref(*state->task);
        {
            auto f = state->work.lock().unwrap();
            *f     = None();
        }
        if (joined.is_err()) co_return Err(Error::Closed());
        co_return rstd::move(joined).unwrap();
    }
};
export class EngineHandle;
export class ResourceContext {
    Arc<ScopeState> scope_;
    Option<Instant> deadline_;

public:
    ResourceContext(Arc<ScopeState> scope, Option<Instant> deadline = {})
        : scope_(rstd::move(scope)), deadline_(deadline) {}
    auto clone() const -> ResourceContext { return ResourceContext(scope_.clone(), deadline_); }
    auto cancellation() const -> Cancellation { return scope_->cancellation.clone(); }
    auto deadline() const -> Option<Instant> { return deadline_; }
    auto runtime() const -> rstd::async::RuntimeHandle { return scope_->runtime.clone(); }
    auto handle() const -> EngineHandle;
    void record_cleanup_error(Error error) const { scope_->cleanup->record(rstd::move(error)); }
    auto usage() const -> ResourceAmount {
        auto f = scope_->budget->fields.lock().unwrap();
        return f->used;
    }
    auto closed() const -> bool {
        auto f = scope_->fields.lock().unwrap();
        return f->closed || scope_->cancellation.is_canceled();
    }
    auto try_reserve(ResourceAmount amount) const -> Result<ResourcePermit> {
        if (closed()) return Err(Error::Closed());
        return lifetchto::try_reserve(scope_->budget.clone(), amount);
    }
    template<class T>
    auto attach(T access, Arc<rstd::dyn<ManagedResource>> owner)
        -> rstd::Result<ResourceLease<T>, Rejected<ResourceRegistration<T>>> {
        auto permit = lifetchto::try_reserve(scope_->budget.clone(),
                                             ResourceAmount { .resources = usize(1) });
        if (permit.is_err())
            return Err(Rejected<ResourceRegistration<T>> {
                rstd::move(permit).unwrap_err(), { rstd::move(access), rstd::move(owner) } });
        auto record = Arc<ResourceRecord>::make(
            owner.clone(), scope_->runtime.clone(), scope_->cleanup.clone(), true);
        record->permit = Some(rstd::move(permit).unwrap());
        auto admitted  = admit_resource(scope_, record.clone());
        if (admitted.is_err())
            return Err(Rejected<ResourceRegistration<T>> {
                rstd::move(admitted).unwrap_err(), { rstd::move(access), rstd::move(owner) } });
        return Ok(ResourceLease<T>(rstd::move(access), rstd::move(record)));
    }
    template<class D>
    auto spawn(D work) -> rstd::Result<TaskControl, Rejected<D>>;
    auto reserve(ResourceAmount amount) const -> coro<Result<ResourcePermit>>;
};
export struct Progress {
    Option<u64> accepted_bytes;
    Option<u64> sent_bytes;
    Option<u64> received_bytes;
    usize       attempts {};
};
export struct ProtocolCompletion {
    Result<empty> result;
    RemoteOutcome remote { RemoteOutcome::Unknown };
    // A dependent operation may own the original error while this result reports closure.
    Option<FailureReport> diagnostic;
};
export struct ProtocolReport {
    rstd::Result<empty, FailureReport> result;
    RemoteOutcome                      remote { RemoteOutcome::Unknown };
    static auto capture(const ProtocolCompletion& completion) -> ProtocolReport {
        using ReportResult = rstd::Result<empty, FailureReport>;
        return { completion.result.is_ok()
                     ? ReportResult(Ok(empty {}))
                     : ReportResult(
                           Err(completion.diagnostic.is_some()
                                   ? completion.diagnostic->clone()
                                   : report_failure(completion.result.as_ref().unwrap_err()))),
                 completion.remote };
    }
};
export struct TransferReport {
    const ProtocolReport     logical;
    const Option<StopReason> stop_requested;
    const Progress           progress;
    const CleanupReport      cleanup;
    TransferReport(ProtocolReport     logical,
                   Option<StopReason> stop,
                   Progress           progress,
                   CleanupReport      cleanup)
        : logical(rstd::move(logical)),
          stop_requested(stop),
          progress(progress),
          cleanup(rstd::move(cleanup)) {}
};
export struct TransferSnapshot {
    TransferPhase      phase;
    bool               output_ready;
    Progress           progress;
    Option<StopReason> stop_requested;
};
export struct TransferOptions {
    Option<Instant> deadline;
    usize           max_attempts { 16 };
};
export struct ChildOptions {
    Option<Instant> deadline;
};
export struct ScopeOptions {};
struct AttemptFields {
    usize used {};
    usize maximum;
    explicit AttemptFields(usize maximum): maximum(maximum) {}
};
struct AttemptBudget {
    rstd::sync::Mutex<AttemptFields> fields;
    explicit AttemptBudget(usize maximum): fields(AttemptFields(maximum)) {}
};
export struct AttemptPermit {
    usize number;
};
struct OperationFields {
    TransferPhase               phase { TransferPhase::Queued };
    bool                        output_ready {};
    Progress                    progress;
    Option<Arc<TransferReport>> report;
};
struct OperationState {
    Arc<ScopeState>                          scope;
    Arc<AttemptBudget>                       attempts;
    Option<Instant>                          deadline;
    rstd::sync::Mutex<OperationFields>       fields { OperationFields {} };
    Event                                    changed;
    Option<rstd::sync::Weak<ResourceRecord>> record;
    OperationState(Arc<ScopeState> scope, Arc<AttemptBudget> attempts, Option<Instant> deadline)
        : scope(rstd::move(scope)), attempts(rstd::move(attempts)), deadline(deadline) {}
};
void stop_operation(const Arc<OperationState>& state, StopReason reason);
export class TransferControl {
    Arc<OperationState> state_;

public:
    explicit TransferControl(Arc<OperationState> state): state_(rstd::move(state)) {}
    auto clone() const -> TransferControl { return TransferControl(state_.clone()); }
    void cancel() const { stop_operation(state_, StopReason::Canceled); }
    auto snapshot() const -> TransferSnapshot {
        auto f            = state_->fields.lock().unwrap();
        auto progress     = f->progress;
        auto attempts     = state_->attempts->fields.lock().unwrap();
        progress.attempts = attempts->used;
        return { f->phase, f->output_ready, progress, state_->scope->cancellation.reason() };
    }
    auto wait() const -> coro<Arc<TransferReport>>;
    auto close() const -> coro<Arc<TransferReport>> {
        cancel();
        return wait();
    }
};
export template<class T>
class PreparedOperation;
export template<class T>
class Transfer;
export template<class T>
class OutputSender;
export template<class T>
using Submission = rstd::Result<Transfer<T>, Rejected<PreparedOperation<T>>>;
template<class T>
auto submit_operation(Arc<ScopeState>,
                      PreparedOperation<T>,
                      TransferOptions,
                      Option<Arc<AttemptBudget>>) -> Submission<T>;
export class TransferContext {
    Arc<OperationState> state_;

public:
    explicit TransferContext(Arc<OperationState> state): state_(rstd::move(state)) {}
    auto clone() const -> TransferContext { return TransferContext(state_.clone()); }
    auto resources() const -> ResourceContext {
        return ResourceContext(state_->scope.clone(), state_->deadline);
    }
    auto cancellation() const -> Cancellation { return state_->scope->cancellation.clone(); }
    auto deadline() const -> Option<Instant> { return state_->deadline; }
    auto control() const -> TransferControl { return TransferControl(state_.clone()); }
    auto begin_attempt() const -> Result<AttemptPermit> {
        if (cancellation().is_canceled()) return Err(stop_error(*cancellation().reason()));
        if (state_->deadline.is_some() && Instant::now() >= *state_->deadline)
            return Err(Error::Timeout());
        usize used;
        {
            auto f = state_->attempts->fields.lock().unwrap();
            if (f->used >= f->maximum) return Err(Error::AttemptsExhausted());
            used = ++f->used;
        }
        {
            auto progress               = state_->fields.lock().unwrap();
            progress->progress.attempts = used;
        }
        return Ok(AttemptPermit { used });
    }
    void progress(Option<u64> accepted, Option<u64> sent, Option<u64> received) const {
        auto f                     = state_->fields.lock().unwrap();
        f->progress.accepted_bytes = accepted;
        f->progress.sent_bytes     = sent;
        f->progress.received_bytes = received;
    }
    auto delay(Duration duration) const -> coro<Result<empty>>;
    template<class T>
    auto submit(PreparedOperation<T> operation, ChildOptions options = {}) const -> Submission<T> {
        auto deadline = state_->deadline;
        if (options.deadline.is_some() && (deadline.is_none() || *options.deadline < *deadline))
            deadline = options.deadline;
        return submit_operation(state_->scope.clone(),
                                rstd::move(operation),
                                TransferOptions { deadline, usize(16) },
                                Some(state_->attempts.clone()));
    }
};
export template<class T>
struct ProtocolDriver {
    template<class Self, class = void>
    struct Api {
        using Trait = ProtocolDriver;
        auto run(TransferContext context, OutputSender<T> output) const
            -> coro<ProtocolCompletion> {
            return rstd::trait_call<0>(this, rstd::move(context), rstd::move(output));
        }
        void request_stop(StopReason reason) const { rstd::trait_call<1>(this, reason); }
        auto release(ReleaseContext context) const -> coro<Result<empty>> {
            return rstd::trait_call<2>(this, context);
        }
    };
    template<class D>
    using Funcs = rstd::TraitFuncs<&D::run, &D::request_stop, &D::release>;
};
template<class T, class D>
struct StoredDriver {
    Arc<D> driver;
    auto   run(TransferContext context, OutputSender<T> output) const {
        return driver->run(rstd::move(context), rstd::move(output));
    }
    void request_stop(StopReason reason) const { driver->request_stop(reason); }
    auto release(ReleaseContext context) const { return driver->release(context); }
};
template<class T>
auto release_prepared(Arc<rstd::dyn<ProtocolDriver<T>>> driver) -> coro<Result<empty>> {
    co_return co_await driver->release({});
}
struct PreparedFlags {
    bool closing {}, accepted {};
};
template<class T>
struct PreparedState {
    Arc<rstd::dyn<ProtocolDriver<T>>>            driver;
    rstd::sync::Mutex<PreparedFlags>             flags { PreparedFlags {} };
    rstd::async::Mutex<empty>                    gate { empty {} };
    Option<typename coro<Result<empty>>::Future> release;
    CloseOutcome                                 outcome;
    bool                                         closed {};
    explicit PreparedState(Arc<rstd::dyn<ProtocolDriver<T>>> driver): driver(rstd::move(driver)) {}
};
template<class T>
auto wait_prepared_close(Arc<PreparedState<T>> state) -> coro<CleanupReport> {
    auto gate = co_await state->gate.lock();
    if (! state->closed) {
        if (state->release.is_none())
            state->release = Some(release_prepared(state->driver.clone()).into_future());
        auto result    = co_await rstd::future::by_ref(*state->release);
        state->release = None();
        state->closed  = true;
        state->outcome.complete(rstd::move(result));
    }
    co_return state->outcome.report();
}
template<class T>
auto close_prepared(Arc<PreparedState<T>> state) -> coro<Result<empty>> {
    (void)co_await wait_prepared_close(state.clone());
    auto result = state->outcome.take_result();
    if (result.is_none()) co_return Err(Error::Contract("close result already taken"_Str));
    co_return result.take().unwrap();
}
export template<class T>
class PreparedOperation {
    Arc<PreparedState<T>> state_;
    template<class U>
    friend auto submit_operation(Arc<ScopeState>,
                                 PreparedOperation<U>,
                                 TransferOptions,
                                 Option<Arc<AttemptBudget>>) -> Submission<U>;

public:
    explicit PreparedOperation(Arc<rstd::dyn<ProtocolDriver<T>>> driver)
        : state_(Arc<PreparedState<T>>::make(rstd::move(driver))) {}
    PreparedOperation(PreparedOperation&&) noexcept = default;
    PreparedOperation(const PreparedOperation&)     = delete;
    auto operator=(PreparedOperation&& other) noexcept -> PreparedOperation& {
        if (this != &other) {
            request_close();
            state_ = rstd::move(other.state_);
        }
        return *this;
    }
    ~PreparedOperation() { request_close(); }
    template<class D>
    static auto make(D driver) -> PreparedOperation;
    auto driver() const -> Arc<rstd::dyn<ProtocolDriver<T>>> { return state_->driver.clone(); }
    void request_close() const {
        if (! state_) return;
        {
            auto f = state_->flags.lock().unwrap();
            if (f->accepted || f->closing) return;
            f->closing = true;
        }
        state_->driver->request_stop(StopReason::Abandoned);
    }
    auto close() const -> coro<Result<empty>> {
        request_close();
        return close_prepared(state_.clone());
    }
    auto wait_closed() const -> coro<CleanupReport> {
        request_close();
        return wait_prepared_close(state_.clone());
    }
    auto take_close_result() -> Option<Result<empty>> { return state_->outcome.take_result(); }
};
template<class T>
struct OutputFields {
    Option<T>     output;
    Option<Error> error;
    bool          published {}, taken {}, reading {};
};
template<class T>
struct OutputState {
    Arc<OperationState>                    operation;
    Arc<rstd::dyn<ProtocolDriver<T>>>      driver;
    rstd::sync::Mutex<OutputFields<T>>     fields { OutputFields<T> {} };
    Option<rstd::async::JoinHandle<empty>> task;
    rstd::async::Mutex<empty>              task_gate { empty {} };
    explicit OutputState(Arc<OperationState> operation, Arc<rstd::dyn<ProtocolDriver<T>>> driver)
        : operation(rstd::move(operation)), driver(rstd::move(driver)) {}
};
export template<class T>
class OutputSender {
    Arc<OutputState<T>> state_;

public:
    explicit OutputSender(Arc<OutputState<T>> state): state_(rstd::move(state)) {}
    auto publish(T output) const -> Result<empty> {
        auto operation = state_->operation->fields.lock().unwrap();
        if (operation->phase != TransferPhase::Running) return Err(Error::Closed());
        if (state_->operation->scope->cancellation.is_canceled())
            return Err(stop_error(*state_->operation->scope->cancellation.reason()));
        {
            auto f = state_->fields.lock().unwrap();
            if (f->published) return Err(Error::OutputTaken());
            f->published = true;
            f->output    = Some(rstd::move(output));
        }
        operation->output_ready = true;
        state_->operation->changed.notify();
        return Ok(empty {});
    }
};
template<class T>
struct OutputReadGuard {
    Arc<OutputState<T>> state;
    ~OutputReadGuard() {
        auto f     = state->fields.lock().unwrap();
        f->reading = false;
    }
};
template<class T>
auto read_output(Arc<OutputState<T>> state) -> coro<Result<T>> {
    {
        auto f = state->fields.lock().unwrap();
        if (f->taken) co_return Err(Error::OutputTaken());
        if (f->reading) co_return Err(Error::Busy());
        f->reading = true;
    }
    OutputReadGuard<T> guard { state.clone() };
    for (;;) {
        auto event = state->operation->changed.listen();
        {
            auto f = state->fields.lock().unwrap();
            if (f->output.is_some()) {
                f->taken = true;
                co_return Ok(f->output.take().unwrap());
            }
        }
        {
            auto f = state->operation->fields.lock().unwrap();
            if (f->report.is_some()) {
                auto output = state->fields.lock().unwrap();
                if (output->error) {
                    output->taken = true;
                    co_return Err(output->error.take().unwrap());
                }
                if ((*f->report)->logical.result.is_err()) co_return Err(Error::OutputTaken());
                co_return Err(Error::Contract(String::make("driver completed without output"_str)));
            }
        }
        co_await rstd::move(event);
    }
}
export template<class T>
class Transfer {
    Arc<OutputState<T>> state_;

public:
    explicit Transfer(Arc<OutputState<T>> state): state_(rstd::move(state)) {}
    Transfer(Transfer&&) noexcept = default;
    Transfer(const Transfer&)     = delete;
    auto operator=(Transfer&& other) noexcept -> Transfer& {
        if (this != &other) {
            if (state_) {
                stop_operation(state_->operation, StopReason::Abandoned);
                state_->operation->scope->cleanup->forward();
            }
            state_ = rstd::move(other.state_);
        }
        return *this;
    }
    ~Transfer() {
        if (state_) {
            stop_operation(state_->operation, StopReason::Abandoned);
            state_->operation->scope->cleanup->forward();
        }
    }
    auto output() const -> coro<Result<T>> { return read_output(state_.clone()); }
    // Errors after output publication remain owned by this transfer, not its report.
    auto take_error() -> Option<Error> {
        auto f = state_->fields.lock().unwrap();
        if (! f->published || ! f->taken) return None();
        return f->error.take();
    }
    auto control() const -> TransferControl { return TransferControl(state_->operation.clone()); }
    auto take_cleanup_errors() -> Vec<Error> {
        return state_->operation->scope->cleanup->take().unwrap_or(Vec<Error> {});
    }
};
template<class T>
struct DriverResource {
    Arc<rstd::dyn<ProtocolDriver<T>>> driver;
    void request_stop(StopReason reason) const { driver->request_stop(reason); }
    auto release(ReleaseContext context) const -> coro<Result<empty>> {
        return driver->release(context);
    }
};
template<class T>
struct OperationResource {
    Arc<OutputState<T>> state;
    void                request_stop(StopReason reason) const {
        stop_operation(state->operation, reason);
        state->driver->request_stop(reason);
    }
    auto release(ReleaseContext) const -> coro<Result<empty>> {
        auto gate = co_await state->task_gate.lock();
        if (state->task.is_some()) {
            auto joined = co_await rstd::future::by_ref(*state->task);
            state->task = None();
            if (joined.is_err()) co_return Err(Error::Closed());
        }
        co_return Ok(empty {});
    }
};
template<class T>
auto run_operation(Arc<OutputState<T>> state) -> coro<empty> {
    auto operation = state->operation.clone();
    {
        auto f   = operation->fields.lock().unwrap();
        f->phase = TransferPhase::Running;
    }
    auto               context = TransferContext(operation.clone());
    ProtocolCompletion logical { Err(Error::Canceled()), RemoteOutcome::Unknown };
    if (context.cancellation().is_canceled() ||
        (operation->deadline.is_some() && Instant::now() >= *operation->deadline)) {
        auto reason = context.cancellation().reason().unwrap_or(StopReason::Deadline);
        stop_operation(operation, reason);
        state->driver->request_stop(reason);
        logical = { Err(stop_error(reason)), RemoteOutcome::NotDispatched };
    } else {
        auto driver =
            state->driver->run(context.clone(), OutputSender<T>(state.clone())).into_future();
        auto canceled = context.cancellation().stopped().into_future();
        if (operation->deadline.is_some()) {
            auto stop = rstd::async::select(rstd::move(canceled),
                                            rstd::async::sleep(remaining(*operation->deadline)));
            auto result =
                co_await rstd::async::select(rstd::future::by_ref(driver), rstd::move(stop));
            if (result.is_left())
                logical = rstd::move(result).unwrap_left();
            else {
                auto stopped = rstd::move(result).unwrap_right();
                auto reason =
                    stopped.is_right() ? StopReason::Deadline : rstd::move(stopped).unwrap_left();
                stop_operation(operation, reason);
                state->driver->request_stop(reason);
                logical = co_await rstd::move(driver);
            }
        } else {
            auto result =
                co_await rstd::async::select(rstd::future::by_ref(driver), rstd::move(canceled));
            if (result.is_left())
                logical = rstd::move(result).unwrap_left();
            else {
                auto reason = rstd::move(result).unwrap_right();
                stop_operation(operation, reason);
                state->driver->request_stop(reason);
                logical = co_await rstd::move(driver);
            }
        }
    }
    if (logical.result.is_err() && logical.result.as_ref().unwrap_err().is_Timeout() &&
        operation->deadline.is_some() && Instant::now() >= *operation->deadline)
        stop_operation(operation, StopReason::Deadline);
    {
        auto f   = operation->fields.lock().unwrap();
        f->phase = TransferPhase::LogicallyFinished;
    }
    {
        auto f = state->fields.lock().unwrap();
        if (! f->published && logical.result.is_ok())
            logical.result = Err(
                Error::Contract(String::make("driver completed without publishing output"_str)));
    }
    seal_scope(operation->scope, false);
    {
        auto f   = operation->fields.lock().unwrap();
        f->phase = TransferPhase::Releasing;
    }
    auto released = co_await state->driver->release({});
    if (released.is_err()) operation->scope->cleanup->record(rstd::move(released).unwrap_err());
    auto cleanup  = co_await drain_scope(operation->scope.clone());
    auto progress = [&] {
        auto f            = operation->fields.lock().unwrap();
        auto progress     = f->progress;
        auto attempts     = operation->attempts->fields.lock().unwrap();
        progress.attempts = attempts->used;
        return progress;
    }();
    auto report = Arc<TransferReport>::make(ProtocolReport::capture(logical),
                                            operation->scope->cancellation.reason(),
                                            progress,
                                            rstd::move(cleanup));
    {
        auto f      = operation->fields.lock().unwrap();
        auto output = state->fields.lock().unwrap();
        if (logical.result.is_err()) output->error = Some(rstd::move(logical.result).unwrap_err());
        f->report = Some(rstd::move(report));
        f->phase  = TransferPhase::Released;
    }
    operation->changed.notify();
    co_return empty {};
}
export class EngineHandle;
export class TransferScope {
    Arc<ScopeState> scope_;

public:
    explicit TransferScope(Arc<ScopeState> scope): scope_(rstd::move(scope)) {}
    TransferScope(TransferScope&&) noexcept = default;
    TransferScope(const TransferScope&)     = delete;
    auto operator=(TransferScope&& other) noexcept -> TransferScope& {
        if (this != &other) {
            request_close();
            if (scope_) scope_->cleanup->forward();
            scope_ = rstd::move(other.scope_);
        }
        return *this;
    }
    ~TransferScope() {
        request_close();
        if (scope_) scope_->cleanup->forward();
    }
    auto resources() const -> ResourceContext { return ResourceContext(scope_.clone()); }
    auto handle() const -> EngineHandle;
    template<class T>
    auto submit(PreparedOperation<T> operation, TransferOptions options = {}) const
        -> Submission<T> {
        return submit_operation(
            scope_.clone(), rstd::move(operation), options, None<Arc<AttemptBudget>>());
    }
    void request_close() const;
    auto close() const -> coro<CleanupReport>;
    auto take_close_errors() -> Vec<Error>;
    void record_cleanup_error(Error error) { scope_->cleanup->record(rstd::move(error)); }
};
export using ScopeHandle = EngineHandle;
export class EngineHandle {
    rstd::sync::Weak<ScopeState> scope_;

public:
    explicit EngineHandle(rstd::sync::Weak<ScopeState> scope): scope_(rstd::move(scope)) {}
    auto clone() const -> EngineHandle { return EngineHandle(scope_.clone()); }
    auto scope(ScopeOptions = {}) const -> Result<TransferScope>;
    auto resources() const -> Result<ResourceContext> {
        auto scope = scope_.upgrade();
        if (! scope) return Err(Error::Closed());
        return Ok(ResourceContext(rstd::move(scope)));
    }
    template<class T>
    auto submit(PreparedOperation<T> operation, TransferOptions options = {}) const
        -> Submission<T> {
        auto scope = scope_.upgrade();
        if (! scope)
            return Err(Rejected<PreparedOperation<T>> { Error::Closed(), rstd::move(operation) });
        return submit_operation(
            rstd::move(scope), rstd::move(operation), options, None<Arc<AttemptBudget>>());
    }
};
export class Engine {
    TransferScope scope_;
    explicit Engine(TransferScope scope): scope_(rstd::move(scope)) {}

public:
    Engine(Engine&&) noexcept                           = default;
    Engine(const Engine&)                               = delete;
    auto        operator=(Engine&&) noexcept -> Engine& = default;
    static auto make(rstd::async::RuntimeHandle runtime, EngineOptions options = {})
        -> Result<Engine>;
    auto handle() const -> EngineHandle { return scope_.handle(); }
    auto resources() const -> ResourceContext { return scope_.resources(); }
    auto scope(ScopeOptions options = {}) const -> Result<TransferScope> {
        return handle().scope(options);
    }
    template<class T>
    auto submit(PreparedOperation<T> operation, TransferOptions options = {}) const
        -> Submission<T> {
        return scope_.submit(rstd::move(operation), options);
    }
    void request_close() const { scope_.request_close(); }
    auto close() const -> coro<CleanupReport> { return scope_.close(); }
    auto take_close_errors() -> Vec<Error> { return scope_.take_close_errors(); }
};
} // namespace lifetchto
export namespace rstd
{
template<class D>
struct Impl<lifetchto::ManagedResource, lifetchto::TaskResource<D>>
    : ImplBase<lifetchto::TaskResource<D>> {
    void request_stop(lifetchto::StopReason reason) const { this->self().request_stop(reason); }
    auto release(lifetchto::ReleaseContext context) const { return this->self().release(context); }
};
template<class T>
struct Impl<lifetchto::ManagedResource, lifetchto::OperationResource<T>>
    : ImplBase<lifetchto::OperationResource<T>> {
    void request_stop(lifetchto::StopReason reason) const { this->self().request_stop(reason); }
    auto release(lifetchto::ReleaseContext context) const { return this->self().release(context); }
};
template<class T, class D>
struct Impl<lifetchto::ProtocolDriver<T>, lifetchto::StoredDriver<T, D>>
    : ImplBase<lifetchto::StoredDriver<T, D>> {
    auto run(lifetchto::TransferContext context, lifetchto::OutputSender<T> output) const {
        return this->self().run(rstd::move(context), rstd::move(output));
    }
    void request_stop(lifetchto::StopReason reason) const { this->self().request_stop(reason); }
    auto release(lifetchto::ReleaseContext context) const { return this->self().release(context); }
};
} // namespace rstd
namespace lifetchto
{
template<class D>
auto run_task(Arc<TaskState<D>> state) -> coro<Result<empty>> {
    auto work = [&] {
        auto f = state->work.lock().unwrap();
        return (*f)->clone();
    }();
    co_return co_await work->run(state->cancellation.clone());
}
template<class D>
auto ResourceContext::spawn(D work) -> rstd::Result<TaskControl, Rejected<D>> {
    auto permit =
        lifetchto::try_reserve(scope_->budget.clone(), ResourceAmount { .tasks = usize(1) });
    if (permit.is_err())
        return Err(Rejected<D> { rstd::move(permit).unwrap_err(), rstd::move(work) });
    auto state  = Arc<TaskState<D>>::make(rstd::move(work));
    auto record = Arc<ResourceRecord>::make(
        Arc<rstd::dyn<ManagedResource>>::make(TaskResource<D> { state.clone() }),
        scope_->runtime.clone(),
        scope_->cleanup.clone());
    auto lock = scope_->fields.lock().unwrap();
    if (lock->closed || scope_->cancellation.is_canceled()) {
        auto f = state->work.lock().unwrap();
        return Err(Rejected<D> { Error::Closed(), f->take().unwrap().try_unwrap().unwrap() });
    }
    record->permit = Some(rstd::move(permit).unwrap());
    state->task    = Some(scope_->runtime.spawn(run_task(state.clone())));
    lock->entries.push(record.clone());
    start_release(record);
    return Ok(TaskControl(rstd::move(record)));
}
template<class T>
template<class D>
auto PreparedOperation<T>::make(D driver) -> PreparedOperation {
    return PreparedOperation(Arc<rstd::dyn<ProtocolDriver<T>>>::make(
        StoredDriver<T, D> { Arc<D>::make(rstd::move(driver)) }));
}
template<class T>
auto submit_operation(Arc<ScopeState>            parent,
                      PreparedOperation<T>       prepared,
                      TransferOptions            options,
                      Option<Arc<AttemptBudget>> inherited) -> Submission<T> {
    {
        auto f = prepared.state_->flags.lock().unwrap();
        if (f->closing || f->accepted)
            return Err(Rejected<PreparedOperation<T>> { Error::Closed(), rstd::move(prepared) });
    }
    auto permit = try_reserve(parent->budget.clone(), ResourceAmount { .operations = usize(1) });
    if (permit.is_err())
        return Err(Rejected<PreparedOperation<T>> { rstd::move(permit).unwrap_err(),
                                                    rstd::move(prepared) });
    auto scope = Arc<ScopeState>::make(
        parent->runtime.clone(), parent->budget.clone(), Some(parent->cleanup.clone()));
    auto attempts = inherited.is_some() ? inherited.take().unwrap()
                                        : Arc<AttemptBudget>::make(options.max_attempts);
    auto operation =
        Arc<OperationState>::make(scope.clone(), rstd::move(attempts), options.deadline);
    auto output = Arc<OutputState<T>>::make(operation.clone(), prepared.driver());
    auto record = Arc<ResourceRecord>::make(
        Arc<rstd::dyn<ManagedResource>>::make(OperationResource<T> { output.clone() }),
        parent->runtime.clone(),
        scope->cleanup.clone());
    {
        auto f = parent->fields.lock().unwrap();
        if (f->closed || parent->cancellation.is_canceled())
            return Err(Rejected<PreparedOperation<T>> { Error::Closed(), rstd::move(prepared) });
        for (usize i; i < f->entries.len();) {
            if (f->entries[i]->finished()) {
                f->entries.remove(i);
            } else
                ++i;
        }
        {
            auto flags      = prepared.state_->flags.lock().unwrap();
            flags->accepted = true;
        }
        record->permit    = Some(rstd::move(permit).unwrap());
        operation->record = Some(record.downgrade());
        output->task      = Some(parent->runtime.spawn(run_operation(output.clone())));
        f->entries.push(record.clone());
        start_release(record);
    }
    return Ok(Transfer<T>(rstd::move(output)));
}
} // namespace lifetchto
