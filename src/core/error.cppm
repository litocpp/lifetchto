module;
#include <rstd/enum.hpp>
export module lifetchto.core:error;
export import rstd;
using namespace rstd::prelude;
using namespace rstd::literals;
export namespace lifetchto
{
using rstd::async::coro;
using rstd::bytes::Bytes;
using rstd::sync::Arc;
using rstd::time::Duration;
using rstd::time::Instant;
class Error {
    RSTD_ENUM(Error,
              (InvalidOptions, (String message;)),
              (Unsupported, (String message;)),
              (Resolve, (rstd::net::LookupError cause;)),
              (Connect, (rstd::io::Error cause;)),
              (Protocol, (String message; Option<Arc<rstd::dyn<rstd::error::Error>>> cause;)),
              (Extension, (String name; Arc<rstd::dyn<rstd::error::Error>> cause;)),
              (BodySource, (String message;)),
              (Redirect, (String message;)),
              (Contract, (String message;)),
              (Timeout),
              (Canceled),
              (Closed),
              (Capacity),
              (Busy),
              (TooLarge),
              (OutputTaken),
              (ReplayUnavailable),
              (AttemptsExhausted),
              (CacheMiss))
public:
    auto clone() const -> Error {
        if (is_InvalidOptions()) return InvalidOptions(as_InvalidOptions().message.clone());
        if (is_Unsupported()) return Unsupported(as_Unsupported().message.clone());
        if (is_Resolve()) return Resolve(as_Resolve().cause.clone());
        if (is_Connect()) return Connect(as_Connect().cause);
        if (is_Protocol())
            return Protocol(as_Protocol().message.clone(), as_Protocol().cause.clone());
        if (is_Extension())
            return Extension(as_Extension().name.clone(), as_Extension().cause.clone());
        if (is_BodySource()) return BodySource(as_BodySource().message.clone());
        if (is_Redirect()) return Redirect(as_Redirect().message.clone());
        if (is_Contract()) return Contract(as_Contract().message.clone());
        if (is_Timeout()) return Timeout();
        if (is_Canceled()) return Canceled();
        if (is_Closed()) return Closed();
        if (is_Capacity()) return Capacity();
        if (is_Busy()) return Busy();
        if (is_TooLarge()) return TooLarge();
        if (is_OutputTaken()) return OutputTaken();
        if (is_ReplayUnavailable()) return ReplayUnavailable();
        if (is_AttemptsExhausted()) return AttemptsExhausted();
        return CacheMiss();
    }
};
template<class T>
using Result = rstd::Result<T, Error>;
template<class Input>
struct Rejected {
    Error error;
    Input input;
};
} // namespace lifetchto
export namespace rstd
{
template<>
struct Impl<fmt::Display, lifetchto::Error> : ImplBase<lifetchto::Error> {
    auto fmt(fmt::Formatter& f) const -> bool {
        const auto& e = this->self();
        if (e.is_InvalidOptions()) return f.write_str(e.as_InvalidOptions().message.as_str());
        if (e.is_Unsupported()) return f.write_str(e.as_Unsupported().message.as_str());
        if (e.is_Resolve()) return f.write_fmt(fmt::Arguments::make("{}", e.as_Resolve().cause));
        if (e.is_Connect()) return f.write_fmt(fmt::Arguments::make("{}", e.as_Connect().cause));
        if (e.is_Protocol()) return f.write_str(e.as_Protocol().message.as_str());
        if (e.is_Extension())
            return f.write_fmt(fmt::Arguments::make(
                "{}: {}", e.as_Extension().name, e.as_Extension().cause.deref()));
        if (e.is_BodySource()) return f.write_str(e.as_BodySource().message.as_str());
        if (e.is_Redirect()) return f.write_str(e.as_Redirect().message.as_str());
        if (e.is_Contract()) return f.write_str(e.as_Contract().message.as_str());
        if (e.is_Timeout()) return f.write_str("transfer deadline expired"_str);
        if (e.is_Canceled()) return f.write_str("transfer canceled"_str);
        if (e.is_Closed()) return f.write_str("transfer owner closed"_str);
        if (e.is_Capacity()) return f.write_str("transfer capacity exceeded"_str);
        if (e.is_Busy()) return f.write_str("another consumer is active"_str);
        if (e.is_TooLarge()) return f.write_str("transfer size exceeded"_str);
        if (e.is_OutputTaken()) return f.write_str("output already taken"_str);
        if (e.is_ReplayUnavailable()) return f.write_str("upload cannot be replayed"_str);
        if (e.is_AttemptsExhausted()) return f.write_str("attempt budget exhausted"_str);
        return f.write_str("cache miss"_str);
    }
};
template<>
struct Impl<fmt::Debug, lifetchto::Error> : Impl<fmt::Display, lifetchto::Error> {};
template<>
struct Impl<error::Error, lifetchto::Error> : DefaultInImpl<error::Error, lifetchto::Error> {
    auto source() const noexcept -> Option<error::ErrorRef> {
        const auto& e = this->self();
        if (e.is_Extension()) return Some(e.as_Extension().cause.deref());
        if (e.is_Protocol() && e.as_Protocol().cause.is_some())
            return Some(e.as_Protocol().cause->deref());
        if (e.is_Resolve()) return Some(dyn<error::Error>::from_ref(e.as_Resolve().cause));
        if (e.is_Connect()) return Some(dyn<error::Error>::from_ref(e.as_Connect().cause));
        return None();
    }
};
} // namespace rstd
