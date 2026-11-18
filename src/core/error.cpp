module lifetchto.core;
import :error;
using namespace rstd::prelude;
using namespace rstd::literals;

auto rstd::Impl<rstd::fmt::Display, lifetchto::FailureReport>::fmt(fmt::Formatter& f) const
    -> bool {
    return f.write_str(this->self().message.as_str());
}
auto rstd::Impl<rstd::fmt::Debug, lifetchto::FailureReport>::fmt(fmt::Formatter& f) const -> bool {
    return f.write_str(this->self().debug.as_str());
}
auto rstd::Impl<rstd::fmt::Display, lifetchto::Error>::fmt(fmt::Formatter& f) const -> bool {
    const auto& e = this->self();
    if (e.is_InvalidOptions()) return f.write_str(e.as_InvalidOptions().message.as_str());
    if (e.is_Unsupported()) return f.write_str(e.as_Unsupported().message.as_str());
    if (e.is_Resolve()) return f.write_fmt(fmt::Arguments::make("{}", e.as_Resolve().cause));
    if (e.is_Connect()) return f.write_fmt(fmt::Arguments::make("{}", e.as_Connect().cause));
    if (e.is_Protocol()) return f.write_str(e.as_Protocol().message.as_str());
    if (e.is_Extension())
        return f.write_fmt(
            fmt::Arguments::make("{}: {}", e.as_Extension().name, e.as_Extension().cause.deref()));
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
auto rstd::Impl<rstd::error::Error, lifetchto::Error>::source() const noexcept
    -> Option<error::ErrorRef> {
    const auto& e = this->self();
    if (e.is_Extension()) return Some(e.as_Extension().cause.deref());
    if (e.is_Protocol() && e.as_Protocol().cause.is_some())
        return Some(e.as_Protocol().cause->deref());
    if (e.is_Resolve()) return Some(dyn<error::Error>::from_ref(e.as_Resolve().cause));
    if (e.is_Connect()) return Some(dyn<error::Error>::from_ref(e.as_Connect().cause));
    return None();
}

namespace lifetchto
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
