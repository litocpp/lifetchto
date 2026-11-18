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
    auto fmt(fmt::Formatter& f) const -> bool;
};
template<>
struct Impl<fmt::Debug, lifetchto::FailureReport> : ImplBase<lifetchto::FailureReport> {
    auto fmt(fmt::Formatter& f) const -> bool;
};
template<>
struct Impl<fmt::Display, lifetchto::Error> : ImplBase<lifetchto::Error> {
    auto fmt(fmt::Formatter& f) const -> bool;
};
template<>
struct Impl<fmt::Debug, lifetchto::Error> : Impl<fmt::Display, lifetchto::Error> {};
template<>
struct Impl<error::Error, lifetchto::Error> : DefaultInImpl<error::Error, lifetchto::Error> {
    auto source() const noexcept -> Option<error::ErrorRef>;
};
} // namespace rstd

export namespace lifetchto
{
auto collect_errors(Vec<Error> errors) -> Result<empty>;
auto report_failure(const Error& error) -> FailureReport;
} // namespace lifetchto
