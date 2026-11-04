export module lifetchto.http:error;
export import lifetchto.core;
export import lihttpto;
#if LITO_FEAT_TLS
export import licrypto;
#endif
using namespace rstd::prelude;
using namespace rstd::literals;
export namespace lifetchto::http
{
using lifetchto::Error;
template<class T>
using Result = lifetchto::Result<T>;
enum class ConnectStage
{
    Dial,
    Tls,
    ProxyConnect,
    Http2Settings,
    ConnectQueued,
    ConnectWrite,
    ConnectHead,
    ConnectPayload,
    TunnelIo
};
auto connect_stage_name(ConnectStage stage) -> ref<str> {
    switch (stage) {
    case ConnectStage::Dial: return "dial"_str;
    case ConnectStage::Tls: return "tls"_str;
    case ConnectStage::ProxyConnect: return "proxy_connect"_str;
    case ConnectStage::Http2Settings: return "http2_settings"_str;
    case ConnectStage::ConnectQueued: return "connect_queued"_str;
    case ConnectStage::ConnectWrite: return "connect_write"_str;
    case ConnectStage::ConnectHead: return "connect_head"_str;
    case ConnectStage::ConnectPayload: return "connect_payload"_str;
    case ConnectStage::TunnelIo: return "tunnel_io"_str;
    }
    return "dial"_str;
}
struct ConnectProgress {
    ConnectStage stage { ConnectStage::Dial };
    u64          session_id {};
    u32          stream_id {};
};
struct InitialPayloadProgress {
    usize total {}, confirmed {};
    // Queued output may still be sent after failure; zero confirmed is not a replay guarantee.
    bool may_have_been_sent {};
};
struct ConnectFailure {
    ConnectStage                   stage;
    u64                            session_id {};
    u32                            stream_id {};
    Option<InitialPayloadProgress> initial_payload;
    auto progress() const -> ConnectProgress { return { stage, session_id, stream_id }; }
};
auto protocol_error(String message, Option<lihttpto::DecodeError> cause = {}) -> Error {
    Option<Box<rstd::dyn<rstd::error::Error>>> owned;
    if (cause.is_some())
        owned = Some(Box<rstd::dyn<rstd::error::Error>>::make(cause.take().unwrap()));
    return Error::Protocol(rstd::move(message), rstd::move(owned));
}
auto http2_error(lihttpto::h2::Error cause) -> Error {
    return Error::Extension("http2"_Str,
                            Box<rstd::dyn<rstd::error::Error>>::make(rstd::move(cause)));
}
#if LITO_FEAT_TLS
auto tls_error(licrypto::Error cause) -> Error {
    return Error::Extension(String::make("tls"_str),
                            Box<rstd::dyn<rstd::error::Error>>::make(rstd::move(cause)));
}
#endif
} // namespace lifetchto::http

export namespace rstd
{
template<>
struct Impl<fmt::Display, lifetchto::http::ConnectFailure>
    : ImplBase<lifetchto::http::ConnectFailure> {
    auto fmt(fmt::Formatter& f) const -> bool {
        return f.write_fmt(
            fmt::Arguments::make("HTTP connection failed at stage {} (session {}, stream {})",
                                 lifetchto::http::connect_stage_name(this->self().stage),
                                 this->self().session_id,
                                 this->self().stream_id));
    }
};
template<>
struct Impl<fmt::Debug, lifetchto::http::ConnectFailure>
    : Impl<fmt::Display, lifetchto::http::ConnectFailure> {};
template<>
struct Impl<error::Error, lifetchto::http::ConnectFailure>
    : DefaultInImpl<error::Error, lifetchto::http::ConnectFailure> {};
} // namespace rstd
namespace lifetchto::http
{
auto connect_failure(Error                          error,
                     ConnectProgress                progress,
                     Option<InitialPayloadProgress> initial_payload = {}) -> Error {
    return rstd::move(error).with_context(Box<rstd::dyn<rstd::error::Error>>::make(ConnectFailure {
        progress.stage, progress.session_id, progress.stream_id, initial_payload }));
}
auto failure_progress(const Error& error, ConnectProgress fallback) -> ConnectProgress {
    if (error.context().is_some()) {
        auto existing = rstd::error::downcast_ref<ConnectFailure>(error.context()->deref());
        if (existing.is_some()) return (*existing)->progress();
    }
    return fallback;
}
} // namespace lifetchto::http
