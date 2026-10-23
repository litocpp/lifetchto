module;
#include <rstd/enum.hpp>
export module lifetchto.http:backend;
export import :request;
export import :response;
using namespace rstd::prelude;
export namespace lifetchto::http
{
class BackendIdentity {
    Arc<empty> token_ { Arc<empty>::make() };

public:
    auto clone() const -> BackendIdentity {
        BackendIdentity id;
        id.token_ = token_.clone();
        return id;
    }
    auto matches(const BackendIdentity& other) const -> bool {
        return Arc<empty>::ptr_eq(token_, other.token_);
    }
};
struct RouteChoice {
    Option<Arc<Route>> route;
    auto               clone() const -> RouteChoice { return { route.clone() }; }
};
struct CacheScope {
    BackendIdentity backend;
    RouteChoice     routing;
    auto            clone() const -> CacheScope { return { backend.clone(), routing.clone() }; }
    auto            matches(const CacheScope& other) const -> bool {
        if (! backend.matches(other.backend)) return false;
        if (routing.route.is_none() || other.routing.route.is_none())
            return routing.route.is_none() && other.routing.route.is_none();
        return Arc<Route>::ptr_eq(*routing.route, *other.routing.route);
    }
};
class PreparedExchange {
    lihttpto::PreparedRequest request_;
    BackendIdentity           backend_;
    UploadMetadata            upload_;
    RouteChoice               route_;
    CacheScope                cache_;
#if LITO_FEAT_HTTP2
    Option<lihttpto::h2::Request> http2_;
#endif

public:
    PreparedExchange(lihttpto::PreparedRequest request,
                     BackendIdentity           backend,
                     UploadMetadata            upload,
                     RouteChoice               route,
                     CacheScope                cache)
        : request_(rstd::move(request)),
          backend_(rstd::move(backend)),
          upload_(rstd::move(upload)),
          route_(rstd::move(route)),
          cache_(rstd::move(cache)) {}
    static auto make(lihttpto::PreparedRequest request,
                     BackendIdentity           backend,
                     UploadMetadata            upload,
                     RouteChoice               route,
                     CacheScope                cache) -> PreparedExchange {
        return PreparedExchange(rstd::move(request),
                                rstd::move(backend),
                                rstd::move(upload),
                                rstd::move(route),
                                rstd::move(cache));
    }
#if LITO_FEAT_HTTP2
    auto request_for_http2() const { return request_.http2_request(); }
    auto http2_request() const -> const Option<lihttpto::h2::Request>& { return http2_; }
    void set_http2(lihttpto::h2::Request request) { http2_ = Some(rstd::move(request)); }
#endif
    auto parts() const -> const lihttpto::RequestParts& { return request_.parts(); }
    auto encoder() const -> lihttpto::RequestEncoder { return request_.encoder(); }
    auto backend_identity() const -> const BackendIdentity& { return backend_; }
    auto upload() const -> const UploadMetadata& { return upload_; }
    auto target() const -> const RouteChoice& { return route_; }
    auto cache_scope() const -> const CacheScope& { return cache_; }
    auto clone() const -> PreparedExchange {
        auto copy = PreparedExchange(
            request_.clone(), backend_.clone(), upload_.clone(), route_.clone(), cache_.clone());
#if LITO_FEAT_HTTP2
        if (http2_.is_some()) copy.http2_ = Some(http2_->clone());
#endif
        return copy;
    }
};
struct ExchangeInput {
    PreparedExchange prepared;
    UploadBody       body;
};
struct ExchangeBackend {
    template<class Self, class = void>
    struct Api {
        using Trait = ExchangeBackend;
        auto prepare(lihttpto::RequestParts parts, UploadMetadata upload, RouteChoice route) const
            -> Result<PreparedExchange> {
            return rstd::trait_call<0>(
                this, rstd::move(parts), rstd::move(upload), rstd::move(route));
        }
        auto operation(ExchangeInput input) const
            -> rstd::Result<PreparedOperation<ResponseMessage>, Rejected<ExchangeInput>> {
            return rstd::trait_call<1>(this, rstd::move(input));
        }
        void request_close() const { rstd::trait_call<2>(this); }
        auto close() const -> coro<Result<empty>> { return rstd::trait_call<3>(this); }
    };
    template<class T>
    using Funcs = rstd::TraitFuncs<&T::prepare, &T::operation, &T::request_close, &T::close>;
};
class BackendBinding {
    RSTD_ENUM(BackendBinding,
              (Owned, (Arc<rstd::dyn<ExchangeBackend>> value;)),
              (Shared, (Arc<rstd::dyn<ExchangeBackend>> value;)))
public:
    auto backend() const -> Arc<rstd::dyn<ExchangeBackend>> {
        return is_Owned() ? as_Owned().value.clone() : as_Shared().value.clone();
    }
    auto clone() const -> BackendBinding {
        return is_Owned() ? Owned(backend()) : Shared(backend());
    }
};
struct NativeState;
class NativeBackend {
    Arc<NativeState>          state_;
    Arc<ResourceLease<empty>> registration_;
    NativeBackend(Arc<NativeState> state, Arc<ResourceLease<empty>> registration);

public:
    NativeBackend(NativeBackend&&) noexcept;
    auto operator=(NativeBackend&&) noexcept -> NativeBackend&;
    NativeBackend(const NativeBackend&) = delete;
    ~NativeBackend();
    static auto make(ResourceContext resources, TransportOptions options = {})
        -> Result<NativeBackend>;
    auto clone() const -> NativeBackend;
    auto prepare(lihttpto::RequestParts parts, UploadMetadata upload, RouteChoice route = {}) const
        -> Result<PreparedExchange>;
    auto operation(ExchangeInput input) const
        -> rstd::Result<PreparedOperation<ResponseMessage>, Rejected<ExchangeInput>>;
    auto backend() const -> Arc<rstd::dyn<ExchangeBackend>>;
    void request_close() const;
    auto close() const -> coro<Result<empty>>;
};
} // namespace lifetchto::http
export namespace rstd
{
template<>
struct Impl<lifetchto::http::ExchangeBackend, lifetchto::http::NativeBackend>
    : ImplBase<lifetchto::http::NativeBackend> {
    auto prepare(lihttpto::RequestParts          parts,
                 lifetchto::http::UploadMetadata upload,
                 lifetchto::http::RouteChoice    route) const {
        return this->self().prepare(rstd::move(parts), rstd::move(upload), rstd::move(route));
    }
    auto operation(lifetchto::http::ExchangeInput input) const {
        return this->self().operation(rstd::move(input));
    }
    void request_close() const { this->self().request_close(); }
    auto close() const { return this->self().close(); }
};
} // namespace rstd
