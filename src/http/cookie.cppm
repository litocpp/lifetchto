module;
#include <rstd/enum.hpp>
export module lifetchto.http:cookie;
export import :request;
using namespace rstd::prelude;
export namespace lifetchto::http
{
struct CookieJar {
    template<class Self, class = void>
    struct Api {
        using Trait = CookieJar;
        auto select(const lihttpto::Url& target, i64 now) const -> Result<lihttpto::CookieHeader> {
            return rstd::trait_call<0>(this, target, now);
        }
        auto store(const lihttpto::Url& origin, const lihttpto::SetCookie& cookie, i64 now) const
            -> Result<empty> {
            return rstd::trait_call<1>(this, origin, cookie, now);
        }
        void request_close() const { rstd::trait_call<2>(this); }
        auto close() const -> coro<Result<empty>> { return rstd::trait_call<3>(this); }
    };
    template<class T>
    using Funcs = rstd::TraitFuncs<&T::select, &T::store, &T::request_close, &T::close>;
};
class CookieBinding {
    RSTD_ENUM(CookieBinding,
              (Owned, (Arc<rstd::dyn<CookieJar>> value;)),
              (Shared, (Arc<rstd::dyn<CookieJar>> value;)))
public:
    auto jar() const -> Arc<rstd::dyn<CookieJar>> {
        return is_Owned() ? as_Owned().value.clone() : as_Shared().value.clone();
    }
    auto clone() const -> CookieBinding { return is_Owned() ? Owned(jar()) : Shared(jar()); }
};
class RequestCookies {
    RSTD_ENUM_DEFAULT(RequestCookies,
                      (Inherit),
                      (Inherit),
                      (Disabled),
                      (Shared, (Arc<rstd::dyn<CookieJar>> value;)))
public:
    auto clone() const -> RequestCookies {
        if (is_Shared()) return Shared(as_Shared().value.clone());
        return is_Disabled() ? Disabled() : Inherit();
    }
};
struct MemoryCookieState;
class MemoryCookieJar {
    Arc<MemoryCookieState> state_;
    explicit MemoryCookieJar(Arc<MemoryCookieState> state);

public:
    MemoryCookieJar(MemoryCookieJar&&) noexcept;
    MemoryCookieJar(const MemoryCookieJar&) = delete;
    auto operator=(MemoryCookieJar&&) noexcept -> MemoryCookieJar&;
    ~MemoryCookieJar();
    static auto make(lihttpto::CookieOptions options = {}) -> Result<MemoryCookieJar>;
    auto        clone() const -> MemoryCookieJar;
    auto        jar() const -> Arc<rstd::dyn<CookieJar>>;
    auto        select(const lihttpto::Url&, i64 now) const -> Result<lihttpto::CookieHeader>;
    auto store(const lihttpto::Url&, const lihttpto::SetCookie&, i64 now) const -> Result<empty>;
    auto snapshot(i64 now) const -> Result<Vec<lihttpto::StoredCookie>>;
    auto import_netscape(ref<str> input, i64 now) const -> Result<empty>;
    auto export_netscape(i64 now) const -> Result<String>;
    auto clear() const -> Result<empty>;
    auto erase(ref<str> domain, ref<str> path, ref<str> name) const -> Result<bool>;
    void request_close() const;
    auto close() const -> coro<Result<empty>>;
};
} // namespace lifetchto::http
export namespace rstd
{
template<>
struct Impl<lifetchto::http::CookieJar, lifetchto::http::MemoryCookieJar>
    : ImplBase<lifetchto::http::MemoryCookieJar> {
    auto select(const lihttpto::Url& url, i64 now) const { return this->self().select(url, now); }
    auto store(const lihttpto::Url& url, const lihttpto::SetCookie& value, i64 now) const {
        return this->self().store(url, value, now);
    }
    void request_close() const { this->self().request_close(); }
    auto close() const { return this->self().close(); }
};
} // namespace rstd
