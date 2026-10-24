export module lifetchto.http:proxy;
export import :route;
using namespace rstd::prelude;
using namespace rstd::literals;
export namespace lifetchto::http
{
enum class ProxyType
{
    Http,
    Https,
    Socks4,
    Socks4a,
    Socks5,
    Socks5h
};
enum class ProxyMode
{
    Disabled,
    Explicit,
    System
};
struct ProxyCapabilities {
    bool http {}, https {}, socks4 {}, socks4a {}, socks5 {}, socks5h {}, system_snapshot {};
};
class ProxyEndpoint {
    struct State {
        lihttpto::Url url;
        ProxyType     type;
        u16           port;
        State(lihttpto::Url url, ProxyType type, u16 port)
            : url(rstd::move(url)), type(type), port(port) {}
    };
    Arc<State> state_;
    explicit ProxyEndpoint(Arc<State> state): state_(rstd::move(state)) {}

public:
    static auto make(lihttpto::Url url) -> Result<ProxyEndpoint> {
        if (url.host().is_none() || url.host()->is_empty() || url.userinfo().is_some() ||
            url.query().is_some() || url.fragment().is_some() ||
            (! url.path().is_empty() && url.path() != "/"_str))
            return Err(Error::InvalidOptions(
                "proxy requires an origin without userinfo, path, query or fragment"_Str));
        auto scheme = url.scheme();
        if (scheme.is_none()) return Err(Error::InvalidOptions("proxy scheme is required"_Str));
        ProxyType type;
        if (*scheme == "http"_str)
            type = ProxyType::Http;
        else if (*scheme == "https"_str)
            type = ProxyType::Https;
        else if (*scheme == "socks4"_str)
            type = ProxyType::Socks4;
        else if (*scheme == "socks4a"_str)
            type = ProxyType::Socks4a;
        else if (*scheme == "socks5"_str)
            type = ProxyType::Socks5;
        else if (*scheme == "socks5h"_str)
            type = ProxyType::Socks5h;
        else
            return Err(Error::Unsupported("unsupported proxy scheme"_Str));
        auto port = url.effective_port();
        if (port.is_none() && url.port().is_some()) {
            auto parsed = rstd::Impl<rstd::str_::FromStr, u16>::from_str(*url.port());
            if (parsed.is_err()) return Err(Error::InvalidOptions("invalid proxy port"_Str));
            port = Some(parsed.unwrap());
        }
        if (port.is_none()) port = Some(u16(1080));
        if (*port == u16()) return Err(Error::InvalidOptions("proxy port must be positive"_Str));
        return Ok(ProxyEndpoint(Arc<State>::make(rstd::move(url), type, *port)));
    }
    auto clone() const -> ProxyEndpoint { return ProxyEndpoint(state_.clone()); }
    auto url() const -> const lihttpto::Url& { return state_->url; }
    auto type() const -> ProxyType { return state_->type; }
    auto port() const -> u16 { return state_->port; }
    auto host_name() const -> String { return String::make(*state_->url.host_name()); }
};
struct ProxyEnvironment {
    Option<String> http, https, all, no_proxy;
    static auto    capture() -> Result<ProxyEnvironment> {
        auto read = [](ref<str> lower, ref<str> upper) -> Result<Option<String>> {
            for (auto name : array<ref<str>, 2> { lower, upper }) {
                auto value = rstd::env::var(name);
                if (value.is_ok()) return Ok(Some(rstd::move(value).unwrap()));
                if (value.as_ref().unwrap_err().kind != rstd::env::VarError::Kind::NotPresent)
                    return Err(Error::InvalidOptions("proxy environment must contain Unicode"_Str));
            }
            return Ok(None<String>());
        };
        auto http = read("http_proxy"_str, "HTTP_PROXY"_str);
        if (http.is_err()) return Err(rstd::move(http).unwrap_err());
        auto https = read("https_proxy"_str, "HTTPS_PROXY"_str);
        if (https.is_err()) return Err(rstd::move(https).unwrap_err());
        auto all = read("all_proxy"_str, "ALL_PROXY"_str);
        if (all.is_err()) return Err(rstd::move(all).unwrap_err());
        auto no = read("no_proxy"_str, "NO_PROXY"_str);
        if (no.is_err()) return Err(rstd::move(no).unwrap_err());
        return Ok(ProxyEnvironment { rstd::move(http).unwrap(),
                                     rstd::move(https).unwrap(),
                                     rstd::move(all).unwrap(),
                                     rstd::move(no).unwrap() });
    }
};
struct ProxyConnectFailure {
    lihttpto::ResponseHead        response;
    Bytes                         body;
    bool                          body_complete {};
    Option<lihttpto::DecodeError> framing;
    Option<Error>                 transport;
};
class ProxyOptions {
    struct Bypass {
        String      host;
        Option<u16> port;
        bool        subdomains {}, all {};
    };
    struct State {
        ProxyMode             mode { ProxyMode::Disabled };
        Option<ProxyEndpoint> http, https, all;
        Vec<Bypass>           bypass;
    };
    Arc<State> state_ { Arc<State>::make() };
    explicit ProxyOptions(Arc<State> state): state_(rstd::move(state)) {}

public:
    ProxyOptions() = default;
    static auto disabled() -> ProxyOptions { return ProxyOptions {}; }
    static auto explicit_proxy(lihttpto::Url url) -> Result<ProxyOptions> {
        auto endpoint = ProxyEndpoint::make(rstd::move(url));
        if (endpoint.is_err()) return Err(rstd::move(endpoint).unwrap_err());
        auto state  = Arc<State>::make();
        state->mode = ProxyMode::Explicit;
        state->all  = Some(rstd::move(endpoint).unwrap());
        return Ok(ProxyOptions(rstd::move(state)));
    }
    static auto system() -> Result<ProxyOptions> {
        auto environment = ProxyEnvironment::capture();
        if (environment.is_err()) return Err(rstd::move(environment).unwrap_err());
        return system(rstd::move(environment).unwrap());
    }
    static auto system(ProxyEnvironment environment) -> Result<ProxyOptions> {
        auto parse = [](Option<String> value) -> Result<Option<ProxyEndpoint>> {
            if (value.is_none() || value->is_empty()) return Ok(None<ProxyEndpoint>());
            auto url = lihttpto::Url::parse(value->as_str());
            if (url.is_err())
                return Err(Error::InvalidOptions("invalid proxy URL in environment snapshot"_Str));
            auto endpoint = ProxyEndpoint::make(rstd::move(url).unwrap());
            if (endpoint.is_err()) return Err(rstd::move(endpoint).unwrap_err());
            return Ok(Some(rstd::move(endpoint).unwrap()));
        };
        auto http = parse(rstd::move(environment.http));
        if (http.is_err()) return Err(rstd::move(http).unwrap_err());
        auto https = parse(rstd::move(environment.https));
        if (https.is_err()) return Err(rstd::move(https).unwrap_err());
        auto all = parse(rstd::move(environment.all));
        if (all.is_err()) return Err(rstd::move(all).unwrap_err());
        auto state   = Arc<State>::make();
        state->mode  = ProxyMode::System;
        state->http  = rstd::move(http).unwrap();
        state->https = rstd::move(https).unwrap();
        state->all   = rstd::move(all).unwrap();
        if (environment.no_proxy.is_some()) {
            auto input = environment.no_proxy->as_str();
            if (input.len() > usize(65536)) return Err(Error::TooLarge());
            usize begin;
            for (usize end; end <= input.len(); ++end) {
                if (end != input.len() && input.as_bytes()[end] != u8(',')) continue;
                auto token = input.get(begin, end).unwrap().trim_ascii();
                begin      = end + usize(1);
                if (token.is_empty()) continue;
                if (token == "*"_str) {
                    state->bypass.push(Bypass { {}, {}, false, true });
                    continue;
                }
                if (token.starts_with("."_str)) token = token.get(usize(1), token.len()).unwrap();
                if (token.find("*"_str).is_some())
                    return Err(Error::InvalidOptions(
                        "NO_PROXY supports only a complete wildcard or host suffix"_Str));
                auto authority = lihttpto::parse_authority(token.as_bytes());
                auto canonical =
                    lihttpto::Url::parse_http(rstd::format("http://{}", token).as_str());
                if (authority.is_err() || canonical.is_err() ||
                    authority->kind == lihttpto::HostKind::Future)
                    return Err(Error::InvalidOptions(
                        "NO_PROXY requires a host with an optional port; CIDR is unsupported"_Str));
                state->bypass.push(Bypass { String::make(*canonical->host()),
                                            authority->port,
                                            authority->kind == lihttpto::HostKind::Name,
                                            false });
            }
        }
        return Ok(ProxyOptions(rstd::move(state)));
    }
    auto clone() const -> ProxyOptions { return ProxyOptions(state_.clone()); }
    auto mode() const -> ProxyMode { return state_->mode; }
    auto enabled() const -> bool {
        return state_->http.is_some() || state_->https.is_some() || state_->all.is_some();
    }
    auto explicit_endpoint() const -> Option<ProxyEndpoint> {
        return state_->mode == ProxyMode::Explicit ? Some(state_->all->clone())
                                                   : None<ProxyEndpoint>();
    }
    auto validate(ProxyCapabilities supported) const -> Result<empty> {
        if (state_->mode == ProxyMode::System && ! supported.system_snapshot)
            return Err(
                Error::Unsupported("backend does not support a shared system proxy snapshot"_Str));
        for (const auto* endpoint : array<const Option<ProxyEndpoint>*, 3> {
                 &state_->http, &state_->https, &state_->all }) {
            if (endpoint->is_none()) continue;
            bool accepted;
            switch ((**endpoint).type()) {
            case ProxyType::Http: accepted = supported.http; break;
            case ProxyType::Https: accepted = supported.https; break;
            case ProxyType::Socks4: accepted = supported.socks4; break;
            case ProxyType::Socks4a: accepted = supported.socks4a; break;
            case ProxyType::Socks5: accepted = supported.socks5; break;
            case ProxyType::Socks5h: accepted = supported.socks5h; break;
            }
            if (! accepted)
                return Err(Error::Unsupported("backend does not support this proxy type"_Str));
        }
        return Ok(empty {});
    }
    auto select(const lihttpto::Url& target) const -> Result<Option<ProxyEndpoint>> {
        auto origin = target.http_origin();
        if (origin.is_none())
            return Err(Error::InvalidOptions("proxy selection requires an HTTP origin"_Str));
        for (const auto& rule : state_->bypass) {
            if (rule.port.is_some() && *rule.port != origin->port) continue;
            if (rule.all || rule.host == origin->host ||
                (rule.subdomains &&
                 origin->host.as_str().ends_with(rstd::format(".{}", rule.host).as_str())))
                return Ok(None<ProxyEndpoint>());
        }
        const auto& specific = origin->scheme == "https"_str ? state_->https : state_->http;
        const auto& selected = specific.is_some() ? specific : state_->all;
        return Ok(selected.is_some() ? Some(selected->clone()) : None<ProxyEndpoint>());
    }
};
} // namespace lifetchto::http
export namespace rstd
{
template<>
struct Impl<fmt::Display, lifetchto::http::ProxyConnectFailure>
    : ImplBase<lifetchto::http::ProxyConnectFailure> {
    auto fmt(fmt::Formatter& f) const -> bool {
        return f.write_fmt(fmt::Arguments::make("proxy CONNECT returned {}",
                                                this->self().response.status.value()));
    }
};
template<>
struct Impl<fmt::Debug, lifetchto::http::ProxyConnectFailure>
    : Impl<fmt::Display, lifetchto::http::ProxyConnectFailure> {};
template<>
struct Impl<error::Error, lifetchto::http::ProxyConnectFailure>
    : DefaultInImpl<error::Error, lifetchto::http::ProxyConnectFailure> {
    auto source() const noexcept -> Option<error::ErrorRef> {
        if (this->self().transport.is_some())
            return Some(dyn<error::Error>::from_ref(*this->self().transport));
        if (this->self().framing.is_some())
            return Some(dyn<error::Error>::from_ref(*this->self().framing));
        return None();
    }
};
} // namespace rstd
