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
auto protocol_error(String message, Option<lihttpto::DecodeError> cause = {}) -> Error {
    Option<Arc<rstd::dyn<rstd::error::Error>>> owned;
    if (cause.is_some())
        owned = Some(Arc<rstd::dyn<rstd::error::Error>>::make(cause.take().unwrap()));
    return Error::Protocol(rstd::move(message), rstd::move(owned));
}
auto http2_error(lihttpto::h2::Error cause) -> Error {
    return Error::Extension("http2"_Str,
                            Arc<rstd::dyn<rstd::error::Error>>::make(rstd::move(cause)));
}
#if LITO_FEAT_TLS
auto tls_error(licrypto::Error cause) -> Error {
    return Error::Extension(String::make("tls"_str),
                            Arc<rstd::dyn<rstd::error::Error>>::make(rstd::move(cause)));
}
#endif
} // namespace lifetchto::http
