export module lifetchto.http:route;
export import :error;
using namespace rstd::prelude;
using namespace rstd::literals;
export namespace lifetchto::http
{
class Route {
    String                     origin_;
    Vec<rstd::net::SocketAddr> addresses_;
    Option<rstd::bytes::Bytes> unix_;
    u64                        generation_;
    Route(String                     origin,
          Vec<rstd::net::SocketAddr> addresses,
          Option<rstd::bytes::Bytes> path,
          u64                        generation)
        : origin_(rstd::move(origin)),
          addresses_(rstd::move(addresses)),
          unix_(rstd::move(path)),
          generation_(generation) {}

public:
    static auto tcp(const lihttpto::Url&       url,
                    Vec<rstd::net::SocketAddr> addresses,
                    u64                        generation = u64()) -> Result<Route> {
        auto origin = url.http_origin();
        if (origin.is_none() || addresses.is_empty() || addresses.len() > usize(64))
            return Err(Error::InvalidOptions(String::make("invalid TCP route"_str)));
        return Ok(Route(origin->serialize(), rstd::move(addresses), {}, generation));
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
    auto origin() const -> ref<str> { return origin_.as_str(); }
    auto addresses() const -> slice<rstd::net::SocketAddr> { return addresses_.as_slice(); }
    auto unix_path() const -> const Option<rstd::bytes::Bytes>& { return unix_; }
    auto generation() const -> u64 { return generation_; }
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
