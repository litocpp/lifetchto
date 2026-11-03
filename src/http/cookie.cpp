module lifetchto.http;
import :cookie;
using namespace rstd::prelude;
using namespace rstd::literals;
namespace lifetchto::http
{
struct CookieFields {
    lihttpto::CookieStorage storage;
    bool                    closed {};
    explicit CookieFields(lihttpto::CookieOptions options): storage(rstd::move(options)) {}
};
struct MemoryCookieState {
    rstd::sync::Mutex<CookieFields> fields;
    explicit MemoryCookieState(lihttpto::CookieOptions options)
        : fields(CookieFields(rstd::move(options))) {}
};
MemoryCookieJar::MemoryCookieJar(Arc<MemoryCookieState> state): state_(rstd::move(state)) {
}
MemoryCookieJar::MemoryCookieJar(MemoryCookieJar&&) noexcept                    = default;
auto MemoryCookieJar::operator=(MemoryCookieJar&&) noexcept -> MemoryCookieJar& = default;
MemoryCookieJar::~MemoryCookieJar()                                             = default;
auto MemoryCookieJar::clone() const -> MemoryCookieJar {
    return MemoryCookieJar(state_.clone());
}
auto MemoryCookieJar::make(lihttpto::CookieOptions options) -> Result<MemoryCookieJar> {
    if (options.max_entries == usize() || options.max_cookie_bytes == usize())
        return Err(Error::InvalidOptions("zero cookie storage limit"_Str));
    return Ok(MemoryCookieJar(Arc<MemoryCookieState>::make(rstd::move(options))));
}
auto MemoryCookieJar::jar() const -> Arc<rstd::dyn<CookieJar>> {
    return Arc<rstd::dyn<CookieJar>>::make(clone());
}
auto MemoryCookieJar::select(const lihttpto::Url& url, i64 now) const
    -> Result<lihttpto::CookieHeader> {
    auto f = state_->fields.lock().unwrap();
    if (f->closed) return Err(Error::Closed());
    return Ok(f->storage.select(url, now));
}
auto MemoryCookieJar::store(const lihttpto::Url&       url,
                            const lihttpto::SetCookie& value,
                            i64                        now) const -> Result<empty> {
    auto f = state_->fields.lock().unwrap();
    if (f->closed) return Err(Error::Closed());
    (void)f->storage.store(value, url, now);
    return Ok(empty {});
}
auto MemoryCookieJar::snapshot(i64 now) const -> Result<Vec<lihttpto::StoredCookie>> {
    auto f = state_->fields.lock().unwrap();
    if (f->closed) return Err(Error::Closed());
    return Ok(f->storage.snapshot(now));
}
auto MemoryCookieJar::import_netscape(ref<str> input, i64 now) const -> Result<empty> {
    auto f = state_->fields.lock().unwrap();
    if (f->closed) return Err(Error::Closed());
    auto parsed = f->storage.import_netscape(input, now);
    if (parsed.is_err())
        return Err(Error::Extension("cookie file"_Str,
                                    Box<rstd::dyn<rstd::error::Error>>::make(parsed.unwrap_err())));
    return Ok(empty {});
}
auto MemoryCookieJar::export_netscape(i64 now) const -> Result<String> {
    auto f = state_->fields.lock().unwrap();
    if (f->closed) return Err(Error::Closed());
    return Ok(f->storage.export_netscape(now));
}
auto MemoryCookieJar::clear() const -> Result<empty> {
    auto f = state_->fields.lock().unwrap();
    if (f->closed) return Err(Error::Closed());
    f->storage.clear();
    return Ok(empty {});
}
auto MemoryCookieJar::erase(ref<str> domain, ref<str> path, ref<str> name) const -> Result<bool> {
    auto f = state_->fields.lock().unwrap();
    if (f->closed) return Err(Error::Closed());
    return Ok(f->storage.erase(domain, path, name));
}
void MemoryCookieJar::request_close() const {
    auto f    = state_->fields.lock().unwrap();
    f->closed = true;
    f->storage.clear();
}
auto cookie_closed() -> coro<Result<empty>> {
    co_return Ok(empty {});
}
auto MemoryCookieJar::close() const -> coro<Result<empty>> {
    request_close();
    return cookie_closed();
}
} // namespace lifetchto::http
