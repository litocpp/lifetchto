export module lifetchto.http:options;
export import :route;
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
#if LITO_FEAT_TLS
class TlsProfile {
    licrypto::tls::Context context_;
    VersionPolicy          version_;
    explicit TlsProfile(licrypto::tls::Context context, VersionPolicy version)
        : context_(rstd::move(context)), version_(version) {}

public:
    static auto make(licrypto::tls::Trust trust = {},
                     VersionPolicy version      = VersionPolicy::Automatic) -> Result<TlsProfile> {
        licrypto::tls::Parameters parameters;
#if LITO_FEAT_HTTP2
        if (version != VersionPolicy::Http1)
            parameters.application_protocols.push(String::make("h2"_str));
#else
        if (version == VersionPolicy::Http2)
            return Err(Error::Unsupported("HTTP/2 requires feature http2"_Str));
#endif
        if (version != VersionPolicy::Http2)
            parameters.application_protocols.push(String::make("http/1.1"_str));
        auto context = licrypto::tls::Context::client(rstd::move(trust), rstd::move(parameters));
        if (context.is_err()) return Err(tls_error(rstd::move(context).unwrap_err()));
        return Ok(TlsProfile(rstd::move(context).unwrap(), version));
    }
    auto clone() const -> TlsProfile { return TlsProfile(context_.clone(), version_); }
    auto version_policy() const -> VersionPolicy { return version_; }
    auto context() const -> licrypto::tls::Context { return context_.clone(); }
};
#endif
struct TransportOptions {
    usize    max_connections { 64 };
    usize    max_connections_per_origin { 6 };
    usize    max_idle { 16 };
    usize    max_idle_per_origin { 2 };
    usize    max_waiters { 128 };
    usize    receive_window { 65536 };
    usize    upload_window { 65536 };
    usize    head_limit { 65536 };
    usize    max_informational { 16 };
    usize    max_http2_streams { 128 };
    Duration connect_timeout { Duration::from_secs(u64(10)) };
    Duration idle_timeout { Duration::from_secs(u64(30)) };
    Duration tls_close_timeout { Duration::from_secs(u64(2)) };
    Duration expect_continue_timeout { Duration::from_secs(u64(1)) };
    Option<rstd::sync::Arc<rstd::dyn<Resolver>>> resolver;
#if LITO_FEAT_TLS
    Option<TlsProfile> tls;
#endif
};
} // namespace lifetchto::http
