export module lifetchto.http:cache_policy;
export import :error;
using namespace rstd::prelude;
using namespace rstd::literals;
export namespace lifetchto::http
{
enum class CacheMode
{
    Disabled,
    Default,
    Revalidate,
    Bypass,
    OnlyIfCached
};
enum class StoreFailure
{
    Bypass,
    Fail
};
enum class CacheProvider
{
    None,
    Store,
    Backend
};
enum class CacheReadiness
{
    Unconfigured,
    Initializing,
    Ready,
    Failed,
    Closed
};
enum class CacheIsolation
{
    BackendRoutePartition,
    Manager
};
enum class CacheCommit
{
    ApplicationEof,
    BackendCompletion
};
enum class CacheOrigin
{
    Unknown,
    Network,
    Store,
    BackendCache
};
enum class NetworkAccess
{
    Unknown,
    None,
    Confirmed
};
enum class CacheValidation
{
    Unknown,
    NotPerformed,
    Confirmed
};
enum class CacheDirective
{
    Disabled,
    Default,
    Revalidate,
    OnlyIfCached
};
struct CacheObservation {
    CacheOrigin     origin { CacheOrigin::Network };
    NetworkAccess   network { NetworkAccess::Confirmed };
    CacheValidation validation { CacheValidation::Unknown };
    auto            from_cache() const -> bool {
        return origin == CacheOrigin::Store || origin == CacheOrigin::BackendCache;
    }
    auto remote() const -> RemoteOutcome {
        if (network == NetworkAccess::None) return RemoteOutcome::NotApplicable;
        return network == NetworkAccess::Confirmed ? RemoteOutcome::Confirmed
                                                   : RemoteOutcome::Unknown;
    }
};
struct CacheCapabilities {
    CacheProvider provider { CacheProvider::None };
    bool configured {}, default_mode {}, revalidate {}, only_if_cached {}, head {}, vary {};
    bool credentials {}, range {}, user_conditions {};
    CacheIsolation isolation { CacheIsolation::BackendRoutePartition };
    bool           store_errors {}, network_observation { true };
    CacheCommit    commit { CacheCommit::ApplicationEof };
    CacheReadiness readiness { CacheReadiness::Unconfigured };
    auto validate(CacheMode mode, ref<str> partition, StoreFailure failure) const -> Result<empty> {
        if (! configured || provider == CacheProvider::None)
            return Err(Error::Unsupported("cache provider is not configured"_Str));
        if (provider == CacheProvider::Backend && isolation == CacheIsolation::Manager &&
            ! partition.is_empty())
            return Err(Error::Unsupported("backend cache does not isolate partitions"_Str));
        if (failure == StoreFailure::Fail && ! store_errors)
            return Err(Error::Unsupported("backend cache cannot report storage failures"_Str));
        if ((mode == CacheMode::Default && ! default_mode) ||
            (mode == CacheMode::Revalidate && ! revalidate) ||
            (mode == CacheMode::OnlyIfCached && ! only_if_cached))
            return Err(Error::Unsupported("cache mode is not supported by the provider"_Str));
        return Ok(empty {});
    }
};
inline auto cache_directive(CacheMode mode) -> CacheDirective {
    if (mode == CacheMode::Default) return CacheDirective::Default;
    if (mode == CacheMode::Revalidate) return CacheDirective::Revalidate;
    if (mode == CacheMode::OnlyIfCached) return CacheDirective::OnlyIfCached;
    return CacheDirective::Disabled;
}
} // namespace lifetchto::http
