export module lifetchto.http:route;
export import :error;
using namespace rstd::prelude;
using namespace rstd::literals;
export namespace lifetchto::http
{
struct TcpDial {
    // Dropping finish must permit close to cancel and join all pending native work.
    // into_stream transfers ownership after finish succeeds; close is the failure barrier.
    template<class Self, class = void>
    struct Api {
        using Trait = TcpDial;
        auto finish() const -> rstd::async::coro<Result<empty>> {
            return rstd::trait_call<0>(this);
        }
        auto into_stream() const -> Result<rstd::async::io::Stream> {
            return rstd::trait_call<1>(this);
        }
        auto close() const -> rstd::async::coro<Result<empty>> { return rstd::trait_call<2>(this); }
    };
    template<class T>
    using Funcs = rstd::TraitFuncs<&T::finish, &T::into_stream, &T::close>;
};
struct TcpConnector {
    template<class Self, class = void>
    struct Api {
        using Trait = TcpConnector;
        auto prepare(rstd::net::SocketAddr address) const -> Result<Arc<rstd::dyn<TcpDial>>> {
            return rstd::trait_call<0>(this, address);
        }
    };
    template<class T>
    using Funcs = rstd::TraitFuncs<&T::prepare>;
};
struct EndpointConnector {
    template<class Self, class = void>
    struct Api {
        using Trait = EndpointConnector;
        auto prepare() const -> Result<Arc<rstd::dyn<TcpDial>>> {
            return rstd::trait_call<0>(this);
        }
    };
    template<class T>
    using Funcs = rstd::TraitFuncs<&T::prepare>;
};
class Route {
    String                                    origin_;
    Vec<rstd::net::SocketAddr>                addresses_;
    Option<rstd::bytes::Bytes>                unix_;
    u64                                       generation_;
    Option<Arc<rstd::dyn<TcpConnector>>>      connector_;
    Option<Arc<rstd::dyn<EndpointConnector>>> endpoint_connector_;
    Route(String                                    origin,
          Vec<rstd::net::SocketAddr>                addresses,
          Option<rstd::bytes::Bytes>                path,
          u64                                       generation,
          Option<Arc<rstd::dyn<TcpConnector>>>      connector          = {},
          Option<Arc<rstd::dyn<EndpointConnector>>> endpoint_connector = {})
        : origin_(rstd::move(origin)),
          addresses_(rstd::move(addresses)),
          unix_(rstd::move(path)),
          generation_(generation),
          connector_(rstd::move(connector)),
          endpoint_connector_(rstd::move(endpoint_connector)) {}

public:
    static auto tcp(const lihttpto::Url&                 url,
                    Vec<rstd::net::SocketAddr>           addresses,
                    u64                                  generation = u64(),
                    Option<Arc<rstd::dyn<TcpConnector>>> connector  = {}) -> Result<Route> {
        auto origin = url.http_origin();
        if (origin.is_none() || addresses.is_empty() || addresses.len() > usize(64))
            return Err(Error::InvalidOptions(String::make("invalid TCP route"_str)));
        return Ok(Route(
            origin->serialize(), rstd::move(addresses), {}, generation, rstd::move(connector)));
    }
    static auto unix_socket(const lihttpto::Url& url,
                            rstd::bytes::Bytes   path,
                            u64                  generation = u64()) -> Result<Route> {
        for (auto byte : path.as_slice())
            if (byte == u8())
                return Err(Error::InvalidOptions(String::make("NUL in Unix path"_str)));
        auto origin = url.http_origin();
        if (origin.is_none() || origin->scheme.as_str() != "http"_str || path.is_empty())
            return Err(Error::Unsupported(String::make("Unix routes require HTTP and a path"_str)));
        return Ok(Route(origin->serialize(), {}, Some(rstd::move(path)), generation));
    }
    static auto custom(const lihttpto::Url&              url,
                       u64                               generation,
                       Arc<rstd::dyn<EndpointConnector>> connector) -> Result<Route> {
        auto origin = url.http_origin();
        if (origin.is_none() || url.userinfo().is_some())
            return Err(Error::InvalidOptions("invalid custom route origin"_Str));
        return Ok(Route(origin->serialize(), {}, {}, generation, {}, Some(rstd::move(connector))));
    }
    auto origin() const -> ref<str> { return origin_.as_str(); }
    auto addresses() const -> slice<rstd::net::SocketAddr> { return addresses_.as_slice(); }
    auto unix_path() const -> const Option<rstd::bytes::Bytes>& { return unix_; }
    auto generation() const -> u64 { return generation_; }
    auto connector() const -> const Option<Arc<rstd::dyn<TcpConnector>>>& { return connector_; }
    auto endpoint_connector() const -> const Option<Arc<rstd::dyn<EndpointConnector>>>& {
        return endpoint_connector_;
    }
};
struct RouteChoice {
    Option<Arc<Route>> route;
    auto               clone() const -> RouteChoice { return { route.clone() }; }
};
struct Resolver {
    template<class Self, class = void>
    struct Api {
        using Trait = Resolver;
        auto resolve(String host, u16 port) const
            -> rstd::async::coro<Result<Vec<rstd::net::SocketAddr>>> {
            return rstd::trait_call<0>(this, rstd::move(host), port);
        }
    };
    template<class T>
    using Funcs = rstd::TraitFuncs<&T::resolve>;
};
} // namespace lifetchto::http
