module;
#include <rstd/enum.hpp>
export module lifetchto.http:backend;
export import :request;
export import :response;
export import :duplex;
export import :traffic;
using namespace rstd::prelude;
using namespace rstd::literals;
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
    CacheDirective            cache_directive_;
    Option<ProxyEndpoint>     proxy_;
#if LITO_FEAT_HTTP2
    Option<lihttpto::h2::Request> http2_;
#endif

public:
    PreparedExchange(lihttpto::PreparedRequest request,
                     BackendIdentity           backend,
                     UploadMetadata            upload,
                     RouteChoice               route,
                     CacheScope                cache,
                     CacheDirective            directive = CacheDirective::Disabled)
        : request_(rstd::move(request)),
          backend_(rstd::move(backend)),
          upload_(rstd::move(upload)),
          route_(rstd::move(route)),
          cache_(rstd::move(cache)),
          cache_directive_(directive) {}
    static auto make(lihttpto::PreparedRequest request,
                     BackendIdentity           backend,
                     UploadMetadata            upload,
                     RouteChoice               route,
                     CacheScope                cache,
                     CacheDirective directive = CacheDirective::Disabled) -> PreparedExchange {
        return PreparedExchange(rstd::move(request),
                                rstd::move(backend),
                                rstd::move(upload),
                                rstd::move(route),
                                rstd::move(cache),
                                directive);
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
    auto cache_directive() const -> CacheDirective { return cache_directive_; }
    auto cache_scope() const -> const CacheScope& { return cache_; }
    auto proxy() const -> const Option<ProxyEndpoint>& { return proxy_; }
    void set_proxy(Option<ProxyEndpoint> proxy) { proxy_ = rstd::move(proxy); }
    auto clone() const -> PreparedExchange {
        auto copy = PreparedExchange(request_.clone(),
                                     backend_.clone(),
                                     upload_.clone(),
                                     route_.clone(),
                                     cache_.clone(),
                                     cache_directive_);
        if (proxy_.is_some()) copy.proxy_ = Some(proxy_->clone());
#if LITO_FEAT_HTTP2
        if (http2_.is_some()) copy.http2_ = Some(http2_->clone());
#endif
        return copy;
    }
};
struct ExchangeInput {
    PreparedExchange            prepared;
    UploadBody                  body;
    Option<BodyTrafficObserver> body_progress;
};
struct BackendCapabilities {
    bool            http1 {}, http2 {}, https {}, unix_socket {}, explicit_routes {};
    bool            unknown_upload_length {}, upload_trailers {}, response_trailers {};
    bool            repeated_request_fields {}, response_version {}, expect_continue {};
    bool            duplex {}, half_close {};
    TcpCapabilities tcp;
    bool tls_custom_ca {}, tls_identity {}, tls_verification {}, low_speed {}, connect_timeout {};
    ProxyCapabilities proxy;
    CacheCapabilities cache;
    auto              validate_cache(CacheDirective                directive,
                                     const lihttpto::RequestParts& parts,
                                     const UploadMetadata&         upload) const -> Result<empty> {
        if (directive == CacheDirective::Disabled) return Ok(empty {});
        auto supported =
            cache.validate(directive == CacheDirective::Default      ? CacheMode::Default
                           : directive == CacheDirective::Revalidate ? CacheMode::Revalidate
                                                                     : CacheMode::OnlyIfCached,
                           ""_str,
                           StoreFailure::Bypass);
        if (supported.is_err()) return supported;
        if (parts.method.as_ref() == "GET"_str && upload.length == Some(u64()) &&
            ((! cache.credentials && (parts.headers.contains("authorization"_str) ||
                                      parts.headers.contains("cookie"_str))) ||
             (! cache.range && parts.headers.contains("range"_str)) ||
             (! cache.user_conditions && (parts.headers.contains("if-none-match"_str) ||
                                          parts.headers.contains("if-modified-since"_str) ||
                                          parts.headers.contains("if-match"_str) ||
                                          parts.headers.contains("if-unmodified-since"_str) ||
                                          parts.headers.contains("if-range"_str)))))
            return Err(Error::Unsupported(
                "backend cache does not support identity, range or user conditional requests"_Str));
        return Ok(empty {});
    }
    auto validate(const lihttpto::RequestParts& parts,
                  const UploadMetadata&         upload,
                  const RouteChoice&            route = {}) const -> Result<empty> {
        if (parts.url.scheme() == Some("https"_str) && ! https)
            return Err(Error::Unsupported("backend does not support HTTPS"_Str));
        if (upload.length.is_none() && ! unknown_upload_length)
            return Err(Error::Unsupported("backend requires a declared upload length"_Str));
        if (upload.trailers.is_some() && ! upload_trailers)
            return Err(Error::Unsupported("backend does not support upload trailers"_Str));
        if (parts.headers.contains("expect"_str) && ! expect_continue)
            return Err(Error::Unsupported(
                "backend does not provide controlled Expect/Continue uploads"_Str));
        if (! repeated_request_fields) {
            for (const auto& field : parts.headers)
                if (parts.headers.get_all(field.name.as_str()).len() > usize(1))
                    return Err(Error::Unsupported(
                        "backend does not preserve repeated request fields"_Str));
        }
        if (route.route.is_some()) {
            if ((*route.route)->unix_path().is_some() && ! unix_socket)
                return Err(Error::Unsupported("backend does not support Unix socket routes"_Str));
            if ((*route.route)->unix_path().is_none() && ! explicit_routes)
                return Err(
                    Error::Unsupported("backend does not support explicit address routes"_Str));
        }
        return Ok(empty {});
    }
};
struct ExchangeBackend {
    template<class Self, class = void>
    struct Api {
        using Trait = ExchangeBackend;
        auto prepare(lihttpto::RequestParts parts,
                     UploadMetadata         upload,
                     RouteChoice            route,
                     CacheDirective         cache) const -> Result<PreparedExchange> {
            return rstd::trait_call<0>(
                this, rstd::move(parts), rstd::move(upload), rstd::move(route), cache);
        }
        auto operation(ExchangeInput input) const
            -> rstd::Result<PreparedOperation<ResponseMessage>, Rejected<ExchangeInput>> {
            return rstd::trait_call<1>(this, rstd::move(input));
        }
        void request_close() const { rstd::trait_call<2>(this); }
        auto close() const -> coro<Result<empty>> { return rstd::trait_call<3>(this); }
        auto capabilities() const -> BackendCapabilities { return rstd::trait_call<4>(this); }
        auto duplex(DuplexRequest input) const
            -> rstd::Result<PreparedOperation<DuplexStream>, Rejected<DuplexRequest>> {
            return rstd::trait_call<5>(this, rstd::move(input));
        }
    };
    template<class T>
    using Funcs = rstd::TraitFuncs<&T::prepare,
                                   &T::operation,
                                   &T::request_close,
                                   &T::close,
                                   &T::capabilities,
                                   &T::duplex>;
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
    auto prepare(lihttpto::RequestParts parts,
                 UploadMetadata         upload,
                 RouteChoice            route = {},
                 CacheDirective cache = CacheDirective::Disabled) const -> Result<PreparedExchange>;
    auto operation(ExchangeInput input) const
        -> rstd::Result<PreparedOperation<ResponseMessage>, Rejected<ExchangeInput>>;
    auto backend() const -> Arc<rstd::dyn<ExchangeBackend>>;
    auto capabilities() const -> BackendCapabilities;
    auto duplex(DuplexRequest input) const
        -> rstd::Result<PreparedOperation<DuplexStream>, Rejected<DuplexRequest>>;
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
                 lifetchto::http::RouteChoice    route,
                 lifetchto::http::CacheDirective cache) const {
        return this->self().prepare(
            rstd::move(parts), rstd::move(upload), rstd::move(route), cache);
    }
    auto operation(lifetchto::http::ExchangeInput input) const {
        return this->self().operation(rstd::move(input));
    }
    void request_close() const { this->self().request_close(); }
    auto close() const { return this->self().close(); }
    auto capabilities() const { return this->self().capabilities(); }
    auto duplex(lifetchto::http::DuplexRequest input) const {
        return this->self().duplex(rstd::move(input));
    }
};
} // namespace rstd
