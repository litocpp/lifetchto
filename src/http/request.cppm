export module lifetchto.http:request;
export import :options;
using namespace rstd::prelude;
export namespace lifetchto::http
{
struct UploadMetadata {
    Option<u64>               length { Some(u64()) };
    Option<lihttpto::Headers> trailers;
    auto clone() const -> UploadMetadata { return { length, trailers.clone() }; }
};
class UploadBody {
    lifetchto::UploadBody     content_;
    Option<lihttpto::Headers> trailers_;

public:
    UploadBody() = default;
    UploadBody(lifetchto::UploadBody content, Option<lihttpto::Headers> trailers = {})
        : content_(rstd::move(content)), trailers_(rstd::move(trailers)) {}
    UploadBody(UploadBody&&) noexcept                    = default;
    UploadBody(const UploadBody&)                        = delete;
    auto operator=(UploadBody&&) noexcept -> UploadBody& = default;
    auto length() const -> Option<u64> { return content_.length(); }
    auto trailers() const -> const Option<lihttpto::Headers>& { return trailers_; }
    auto take_trailers() -> Option<lihttpto::Headers> { return trailers_.take(); }
    auto next(usize window) -> coro<Result<Option<Bytes>>> { return content_.next(window); }
    void cancel() const { content_.cancel(); }
    auto close() const -> coro<Result<rstd::empty>> { return content_.close(); }
};
class UploadPlan {
    lifetchto::UploadPlan     content_;
    Option<lihttpto::Headers> trailers_;

public:
    UploadPlan() = default;
    UploadPlan(lifetchto::UploadPlan content, Option<lihttpto::Headers> trailers = {})
        : content_(rstd::move(content)), trailers_(rstd::move(trailers)) {}
    UploadPlan(UploadPlan&&) noexcept                           = default;
    UploadPlan(const UploadPlan&)                               = delete;
    auto        operator=(UploadPlan&&) noexcept -> UploadPlan& = default;
    static auto empty() -> UploadPlan { return {}; }
    static auto bytes(Bytes bytes, Option<lihttpto::Headers> trailers = {}) -> UploadPlan {
        return UploadPlan(lifetchto::UploadPlan::bytes(rstd::move(bytes)), rstd::move(trailers));
    }
    static auto once(lifetchto::UploadBody body, Option<lihttpto::Headers> trailers = {})
        -> UploadPlan {
        return UploadPlan(lifetchto::UploadPlan::once(rstd::move(body)), rstd::move(trailers));
    }
    static auto repeatable(lifetchto::UploadMetadata                metadata,
                           Arc<rstd::dyn<lifetchto::UploadFactory>> factory,
                           Option<lihttpto::Headers>                trailers = {}) -> UploadPlan {
        return UploadPlan(lifetchto::UploadPlan::repeatable(metadata, rstd::move(factory)),
                          rstd::move(trailers));
    }
    auto metadata() const -> UploadMetadata { return { content_.length(), trailers_.clone() }; }
    auto length() const -> Option<u64> { return content_.length(); }
    auto replayable() const -> bool { return content_.replayable(); }
    auto open(TransferContext context) const -> coro<Result<UploadBody>>;
    void request_close() const { content_.request_close(); }
    auto close() const -> coro<Result<rstd::empty>> { return content_.close(); }
};
using ExecutionRequest = lihttpto::RequestMessage<UploadPlan>;
} // namespace lifetchto::http
namespace lifetchto::http
{
class ExchangeRequest {
    lihttpto::RequestMessage<UploadBody> message_;

public:
    explicit ExchangeRequest(lihttpto::RequestMessage<UploadBody> message)
        : message_(rstd::move(message)) {}
    auto parts() const -> const lihttpto::RequestParts& { return message_.parts; }
    auto body() -> UploadBody& { return message_.body; }
};
} // namespace lifetchto::http
