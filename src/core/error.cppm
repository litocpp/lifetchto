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
struct FailureReport {
    String message;
    String debug;
    auto   clone() const -> FailureReport { return { message.clone(), debug.clone() }; }
};
class Error {
    RSTD_ENUM(Error,
              (InvalidOptions, (String message;)),
              (Unsupported, (String message;)),
              (Resolve, (rstd::net::LookupError cause;)),
              (Connect, (rstd::io::Error cause;)),
              (Protocol, (String message; Option<Box<rstd::dyn<rstd::error::Error>>> cause;)),
              (Extension, (String name; Box<rstd::dyn<rstd::error::Error>> cause;)),
              (BodySource, (String message;)),
              (Redirect, (String message;)),
              (Contract, (String message;)),
              (Multiple, (Vec<Error> errors;)),
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
private:
    friend struct CleanupState;
    Option<u64>                                cleanup_id_;
    Option<Box<rstd::dyn<rstd::error::Error>>> context_;

public:
    auto context() const -> const Option<Box<rstd::dyn<rstd::error::Error>>>& { return context_; }
    auto with_context(Box<rstd::dyn<rstd::error::Error>> context) && -> Error {
        context_ = Some(rstd::move(context));
        return rstd::move(*this);
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
struct Impl<fmt::Display, lifetchto::FailureReport> : ImplBase<lifetchto::FailureReport> {
    auto fmt(fmt::Formatter& f) const -> bool { return f.write_str(this->self().message.as_str()); }
};
template<>
struct Impl<fmt::Debug, lifetchto::FailureReport> : ImplBase<lifetchto::FailureReport> {
    auto fmt(fmt::Formatter& f) const -> bool { return f.write_str(this->self().debug.as_str()); }
};
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
        if (e.is_Multiple()) {
            bool first = true;
            for (const auto& error : e.as_Multiple().errors) {
                if (! first && ! f.write_str("; "_str)) return false;
                first = false;
                if (! f.write_fmt(fmt::Arguments::make("{}", error))) return false;
            }
            return true;
        }
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

export namespace lifetchto
{
auto collect_errors(Vec<Error> errors) -> Result<empty> {
    if (errors.is_empty()) return Ok(empty {});
    if (errors.len() == usize(1)) return Err(errors.remove(usize()));
    return Err(Error::Multiple(rstd::move(errors)));
}
auto report_failure(const Error& error) -> FailureReport {
    return { rstd::format("{}", error), rstd::format("{:?}", error) };
}
} // namespace lifetchto
