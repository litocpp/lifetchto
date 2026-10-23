module lifetchto.http;
import :request;
using namespace rstd::prelude;
namespace lifetchto::http
{
auto open_body(coro<Result<lifetchto::UploadBody>> opening, Option<lihttpto::Headers> trailers)
    -> coro<Result<UploadBody>> {
    auto body = co_await rstd::move(opening);
    if (body.is_err()) co_return Err(rstd::move(body).unwrap_err());
    co_return Ok(UploadBody(rstd::move(body).unwrap(), rstd::move(trailers)));
}
auto UploadPlan::open(TransferContext context) const -> coro<Result<UploadBody>> {
    return open_body(content_.open(rstd::move(context)), trailers_.clone());
}
} // namespace lifetchto::http
