export module lifetchto.http:options;
export import :route;
export import :proxy;
using namespace rstd::prelude;
using namespace rstd::literals;
export namespace lifetchto::http
{
using rstd::time::Duration;
using rstd::time::Instant;
enum class VersionPolicy
{
    Http1,
    Automatic,
    Http2
};
enum class TlsVerification
{
    Strict,
    ChainOnly,
    Disabled
};
struct TcpCapabilities {
    bool nodelay {}, keepalive {}, timing {};
};
struct TcpOptions {
    Option<bool>                    nodelay;
    Option<rstd::net::TcpKeepalive> keepalive;
    auto specified() const -> bool { return nodelay.is_some() || keepalive.is_some(); }
    auto validate(TcpCapabilities capabilities) const -> Result<empty> {
        if ((nodelay.is_some() && ! capabilities.nodelay) ||
            (keepalive.is_some() && ! capabilities.keepalive))
            return Err(Error::Unsupported("backend cannot configure TCP nodelay/keepalive"_Str));
        if (keepalive.is_some()) {
            auto valid = keepalive->validate();
            if (valid.is_err()) return Err(Error::Connect(valid.unwrap_err()));
            if ((keepalive->idle.is_some() || keepalive->interval.is_some()) &&
                ! capabilities.timing)
                return Err(Error::Unsupported("backend cannot configure keepalive timing"_Str));
        }
        return Ok(empty {});
    }
};
struct LowSpeedOptions {
    u64      bytes_per_second;
    Duration window;
    auto     validate() const -> Result<empty> {
        if (bytes_per_second == u64() || bytes_per_second > u64(0x7fffffff) || window.is_zero() ||
            window.subsec_nanos() != u32() || window.as_secs() > u64(0x7fffffff))
            return Err(Error::InvalidOptions(
                "low-speed rate and whole-second window must be positive"_Str));
        return Ok(empty {});
    }
};
struct TlsIdentity {
    Bytes certificate_chain, private_key;
    auto clone() const -> TlsIdentity { return { certificate_chain.clone(), private_key.clone() }; }
};
struct TlsOptions {
    TlsVerification     verification { TlsVerification::Strict };
    bool                use_system_roots { true };
    Vec<Bytes>          ca_pem;
    Option<TlsIdentity> identity;
    auto                clone() const -> TlsOptions {
        Vec<Bytes> certificates;
        for (const auto& certificate : ca_pem) certificates.push(certificate.clone());
        return { verification,
                 use_system_roots,
                 rstd::move(certificates),
                 identity.is_some() ? Some(identity->clone()) : None<TlsIdentity>() };
    }
};
#if LITO_FEAT_TLS
class TlsProfile {
    licrypto::tls::Context context_;
    VersionPolicy          version_;
    explicit TlsProfile(licrypto::tls::Context context, VersionPolicy version)
        : context_(rstd::move(context)), version_(version) {}

public:
    auto        clone() const -> TlsProfile { return TlsProfile(context_.clone(), version_); }
    static auto make(TlsOptions options = {}, VersionPolicy version = VersionPolicy::Automatic)
        -> Result<TlsProfile> {
        licrypto::tls::Trust trust;
        trust.use_system_roots = options.use_system_roots;
        trust.ca_pem           = rstd::move(options.ca_pem);
        trust.verify_peer      = options.verification != TlsVerification::Disabled;
        licrypto::tls::Parameters parameters;
        parameters.verify_hostname = options.verification == TlsVerification::Strict;
#if LITO_FEAT_HTTP2
        if (version != VersionPolicy::Http1) parameters.application_protocols.push("h2"_Str);
#else
        if (version == VersionPolicy::Http2)
            return Err(Error::Unsupported("HTTP/2 requires feature http2"_Str));
#endif
        if (version != VersionPolicy::Http2) parameters.application_protocols.push("http/1.1"_Str);
        Option<licrypto::tls::Identity> identity;
        if (options.identity.is_some()) {
            auto parsed =
                licrypto::tls::Identity::from_pem(rstd::move(options.identity->certificate_chain),
                                                  rstd::move(options.identity->private_key));
            if (parsed.is_err()) return Err(tls_error(rstd::move(parsed).unwrap_err()));
            identity = Some(rstd::move(parsed).unwrap());
        }
        auto context = licrypto::tls::Context::client(
            rstd::move(trust), rstd::move(parameters), identity.is_some() ? &*identity : nullptr);
        if (context.is_err()) return Err(tls_error(rstd::move(context).unwrap_err()));
        return Ok(TlsProfile(rstd::move(context).unwrap(), version));
    }
    auto version_policy() const -> VersionPolicy { return version_; }
    auto context() const -> licrypto::tls::Context { return context_.clone(); }
};
#endif
struct TransportOptions {
    ProxyOptions            proxy;
    TcpOptions              tcp;
    Option<LowSpeedOptions> low_speed;
    usize                   max_connections { 64 };
    usize                   max_connections_per_origin { 6 };
    usize                   max_idle { 16 };
    usize                   max_idle_per_origin { 2 };
    usize                   max_waiters { 128 };
    usize                   receive_window { 65536 };
    usize                   upload_window { 65536 };
    usize                   head_limit { 65536 };
    usize                   max_informational { 16 };
    usize                   max_http2_streams { 128 };
    Duration                connect_timeout { Duration::from_secs(u64(10)) };
    Duration                idle_timeout { Duration::from_secs(u64(30)) };
    Duration                tls_close_timeout { Duration::from_secs(u64(2)) };
    Duration                expect_continue_timeout { Duration::from_secs(u64(1)) };
    Option<rstd::sync::Arc<rstd::dyn<Resolver>>> resolver;
#if LITO_FEAT_TLS
    Option<TlsProfile> tls;
#endif
};
} // namespace lifetchto::http
