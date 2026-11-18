export module lifetchto.http:client;
export import :cache;
export import :cookie;
export import :upgrade;
using namespace rstd::prelude;
export namespace lifetchto::http
{
enum class RedirectAction
{
    Follow,
    Return,
    Fail
};
struct RedirectContext {
    const lihttpto::RequestParts& request;
    const lihttpto::ResponseHead& response;
    const lihttpto::Url&          target;
    usize                         hops;
};
struct RedirectDecider {
    template<class Self, class = void>
    struct Api {
        using Trait = RedirectDecider;
        auto decide(const RedirectContext& context) const -> Result<RedirectAction> {
            return rstd::trait_call<0>(this, context);
        }
    };
    template<class T>
    using Funcs = rstd::TraitFuncs<&T::decide>;
};
struct RetryContext {
    const lihttpto::RequestParts& request;
    const Error*                  error;
    const lihttpto::ResponseHead* response;
    RemoteOutcome                 remote;
    usize                         retries;
    Duration                      proposed_delay;
};
struct RetryDecision {
    bool     retry {};
    Duration delay;
};
struct RetryDecider {
    template<class Self, class = void>
    struct Api {
        using Trait = RetryDecider;
        auto decide(const RetryContext& context) const -> Result<RetryDecision> {
            return rstd::trait_call<0>(this, context);
        }
    };
    template<class T>
    using Funcs = rstd::TraitFuncs<&T::decide>;
};
struct RedirectPolicy {
    usize       max_hops { 10 };
    bool        allow_cross_origin {}, allow_https_downgrade {}, rewrite_post_301_302 { true };
    Vec<String> remove_on_cross_origin;
    Option<Arc<rstd::dyn<RedirectDecider>>> decider;
    auto                                    clone() const -> RedirectPolicy;
};
struct RetryPolicy {
    usize       max_retries { 3 }, connect_retries { 3 }, status_retries { 3 };
    Vec<String> methods;
    Vec<u16>    statuses;
    Duration    backoff { Duration::from_millis(u64(100)) },
        max_backoff { Duration::from_secs(u64(10)) }, jitter;
    bool                                 respect_retry_after { true }, business_idempotent {};
    Option<Arc<rstd::dyn<RetryDecider>>> decider;
    auto                                 clone() const -> RetryPolicy;
};
struct ExecutionPolicy {
    Option<RedirectPolicy> redirect;
    Option<RetryPolicy>    retry;
    CacheMode              cache { CacheMode::Disabled };
    auto                   clone() const -> ExecutionPolicy;
};
struct RouteProvider {
    template<class Self, class = void>
    struct Api {
        using Trait = RouteProvider;
        auto select(lihttpto::RequestParts parts, TransferContext context) const
            -> coro<Result<RouteChoice>> {
            return rstd::trait_call<0>(this, rstd::move(parts), rstd::move(context));
        }
        void request_close() const { rstd::trait_call<1>(this); }
        auto close() const -> coro<Result<empty>> { return rstd::trait_call<2>(this); }
    };
    template<class T>
    using Funcs = rstd::TraitFuncs<&T::select, &T::request_close, &T::close>;
};
struct Routing {
    Option<Arc<Route>>                    route;
    Option<Arc<rstd::dyn<RouteProvider>>> shared_provider;
    auto clone() const -> Routing { return { route.clone(), shared_provider.clone() }; }
};
struct AttemptReport {
    String                target;
    Option<u16>           status;
    RemoteOutcome         remote;
    Progress              progress;
    Option<FailureReport> error;
    auto                  clone() const -> AttemptReport;
};
struct ExecutionFacts {
    Option<lihttpto::RequestParts> request;
    bool                           from_cache {};
    CacheObservation               cache { CacheOrigin::Unknown,
                                           NetworkAccess::Unknown,
                                           CacheValidation::Unknown };
    bool                           history_truncated {};
    usize                          redirects {}, attempts {};
    Vec<AttemptReport>             history;
    Instant                        started { Instant::now() };
    Option<Instant>                head_received;
    BodyTraffic                    body_progress;
    auto                           clone() const -> ExecutionFacts;
};
struct ExecutionReport {
    ExecutionFacts      facts;
    Arc<TransferReport> transfer;
};
struct ExecutionObserverFields {
    ExecutionFacts          facts;
    Option<TransferControl> control;
    BodyTrafficObserver     body_progress;
};
struct ExecutionObserverState {
    rstd::sync::Mutex<ExecutionObserverFields> fields { ExecutionObserverFields {} };
};
class Client;
class ExecutionObserver {
    Arc<ExecutionObserverState> state_ { Arc<ExecutionObserverState>::make() };

public:
    auto clone() const -> ExecutionObserver {
        ExecutionObserver value;
        value.state_ = state_.clone();
        return value;
    }
    auto snapshot() const -> ExecutionFacts;
    auto wait() const -> coro<Result<ExecutionReport>>;

private:
    friend class Client;
    auto state() const -> Arc<ExecutionObserverState> { return state_.clone(); }
};
struct ExecuteOptions {
    TransferOptions           transfer;
    Option<ExecutionPolicy>   policy;
    Option<Routing>           routing;
    String                    partition;
    Option<ExecutionObserver> report;
    Option<TotalTimeout>      total_timeout;
    RequestCookies            cookies;
    auto                      clone() const -> ExecuteOptions;
};
struct ClientConfig {
    ExecutionPolicy       policy;
    Option<CacheBinding>  cache;
    StoreFailure          store_failure { StoreFailure::Bypass };
    Option<Duration>      total_timeout { Some(Duration::from_secs(u64(30))) };
    usize                 drain_limit { 65536 };
    Duration              drain_timeout { Duration::from_secs(u64(1)) };
    Option<CookieBinding> cookies;
    Routing               routing;
    auto                  validate() const -> Result<empty>;
    auto                  clone() const -> ClientConfig;
};
struct ClientState;
class Client {
    Arc<ClientState>          state_;
    Arc<ResourceLease<empty>> registration_;
    Client(Arc<ClientState> state, Arc<ResourceLease<empty>> registration);

public:
    Client(Client&&) noexcept;
    Client(const Client&) = delete;
    auto operator=(Client&& other) noexcept -> Client&;
    ~Client();
    static auto make(EngineHandle engine, BackendBinding backend, ClientConfig config = {})
        -> Result<Client>;
    auto clone() const -> Client;
    auto policy() const -> ExecutionPolicy;
    auto capabilities() const -> BackendCapabilities;
    auto cache_capabilities() const -> CacheCapabilities;
    auto resources() const -> ResourceContext;
    auto prepare_connection(DuplexRequest, DuplexOptions = {}) const
        -> rstd::Result<ConnectionPlan, Rejected<DuplexRequest>>;
    auto execute(ExecutionRequest request, ExecuteOptions options = {}) const
        -> rstd::Result<Transfer<ResponseMessage>, Rejected<ExecutionRequest>>;
    auto duplex(DuplexRequest request, DuplexOptions options = {}) const
        -> rstd::Result<PendingDuplex, Rejected<DuplexRequest>>;
    auto upgrade(UpgradeRequest request, UpgradeOptions options = {}) const
        -> rstd::Result<PendingUpgrade, Rejected<UpgradeRequest>>;
    void request_close() const;
    auto close() const -> coro<CleanupReport>;
    auto take_close_errors() -> Vec<Error>;
};
} // namespace lifetchto::http
