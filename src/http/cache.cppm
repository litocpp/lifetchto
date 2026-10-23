module;
#include <rstd/enum.hpp>
export module lifetchto.http:cache;
export import :backend;
using namespace rstd::prelude;
using namespace rstd::literals;
export namespace lifetchto::http
{
auto cache_now() -> i64 {
    return rstd::time::SystemTime::now().as_unix_time().seconds;
}
struct CacheKey {
    String     target;
    String     method;
    String     partition;
    CacheScope scope;
    auto       clone() const -> CacheKey {
        return { target.clone(), method.clone(), partition.clone(), scope.clone() };
    }
    auto matches(const CacheKey& other) const -> bool {
        return target == other.target && method == other.method && partition == other.partition &&
               scope.matches(other.scope);
    }
};
struct CacheTarget {
    String     target;
    String     partition;
    CacheScope scope;
};
struct CacheMetadata {
    lihttpto::RequestParts request;
    lihttpto::ResponseHead head;
    lihttpto::Headers      trailers;
    lihttpto::CachePolicy  policy;
    Bytes                  variant;
    auto                   clone() const -> CacheMetadata {
        return { request.clone(),
                 head.clone(),
                 trailers.clone(),
                 { policy.fields.clone(),
                   policy.request_time,
                   policy.response_time,
                   policy.date_anchor },
                 variant.clone() };
    }
};
struct CacheSnapshot {
    template<class Self, class = void>
    struct Api {
        using Trait = CacheSnapshot;
        auto metadata() const -> const CacheMetadata& { return rstd::trait_call<0>(this); }
        auto open(TransferContext context) const -> coro<Result<ResponseBody>> {
            return rstd::trait_call<1>(this, rstd::move(context));
        }
    };
    template<class T>
    using Funcs = rstd::TraitFuncs<&T::metadata, &T::open>;
};
class CacheEntry {
    u64                           id_;
    Arc<rstd::dyn<CacheSnapshot>> snapshot_;

public:
    CacheEntry(u64 id, Arc<rstd::dyn<CacheSnapshot>> snapshot)
        : id_(id), snapshot_(rstd::move(snapshot)) {}
    auto clone() const -> CacheEntry { return { id_, snapshot_.clone() }; }
    auto id() const -> u64 { return id_; }
    auto metadata() const -> const CacheMetadata& { return snapshot_->metadata(); }
    auto open(TransferContext context) const -> coro<Result<ResponseBody>>;
};
struct CacheLookup {
    u64             generation;
    Vec<CacheEntry> candidates;
};
struct CacheWriteSpec {
    CacheKey      key;
    u64           generation;
    CacheMetadata metadata;
    Option<u64>   replacing;
};
enum class CommitResult
{
    Published,
    Conflict
};
struct CacheTransaction {
    template<class Self, class = void>
    struct Api {
        using Trait = CacheTransaction;
        auto write(Bytes bytes, TransferContext context) const -> coro<Result<empty>> {
            return rstd::trait_call<0>(this, rstd::move(bytes), rstd::move(context));
        }
        auto commit(lihttpto::Headers trailers, TransferContext context) const
            -> coro<Result<CommitResult>> {
            return rstd::trait_call<1>(this, rstd::move(trailers), rstd::move(context));
        }
        void request_abort() const { rstd::trait_call<2>(this); }
        auto abort() const -> coro<Result<empty>> { return rstd::trait_call<3>(this); }
    };
    template<class T>
    using Funcs = rstd::TraitFuncs<&T::write, &T::commit, &T::request_abort, &T::abort>;
};
class CacheWriter {
    Arc<rstd::dyn<CacheTransaction>> transaction_;

public:
    explicit CacheWriter(Arc<rstd::dyn<CacheTransaction>> transaction)
        : transaction_(rstd::move(transaction)) {}
    CacheWriter(CacheWriter&&) noexcept = default;
    CacheWriter(const CacheWriter&)     = delete;
    auto operator=(CacheWriter&& other) noexcept -> CacheWriter& {
        if (this != &other) {
            request_abort();
            transaction_ = rstd::move(other.transaction_);
        }
        return *this;
    }
    ~CacheWriter() { request_abort(); }
    auto write(Bytes bytes, TransferContext context) const -> coro<Result<empty>>;
    auto commit(lihttpto::Headers trailers, TransferContext context) const
        -> coro<Result<CommitResult>>;
    void request_abort() const {
        if (transaction_) transaction_->request_abort();
    }
    auto abort() const -> coro<Result<empty>>;
};
struct CacheStore {
    template<class Self, class = void>
    struct Api {
        using Trait = CacheStore;
        auto lookup(CacheKey key, TransferContext context) const -> coro<Result<CacheLookup>> {
            return rstd::trait_call<0>(this, rstd::move(key), rstd::move(context));
        }
        auto begin_write(CacheWriteSpec spec, TransferContext context) const
            -> coro<Result<CacheWriter>> {
            return rstd::trait_call<1>(this, rstd::move(spec), rstd::move(context));
        }
        auto update(u64 id, u64 generation, CacheMetadata metadata, TransferContext context) const
            -> coro<Result<CommitResult>> {
            return rstd::trait_call<2>(
                this, id, generation, rstd::move(metadata), rstd::move(context));
        }
        auto invalidate(CacheTarget target, TransferContext context) const -> coro<Result<empty>> {
            return rstd::trait_call<3>(this, rstd::move(target), rstd::move(context));
        }
        void request_close() const { rstd::trait_call<4>(this); }
        auto close() const -> coro<Result<empty>> { return rstd::trait_call<5>(this); }
    };
    template<class T>
    using Funcs = rstd::TraitFuncs<&T::lookup,
                                   &T::begin_write,
                                   &T::update,
                                   &T::invalidate,
                                   &T::request_close,
                                   &T::close>;
};
class StoreBinding {
    RSTD_ENUM(StoreBinding,
              (Owned, (Arc<rstd::dyn<CacheStore>> value;)),
              (Shared, (Arc<rstd::dyn<CacheStore>> value;)))
public:
    auto store() const -> Arc<rstd::dyn<CacheStore>> {
        return is_Owned() ? as_Owned().value.clone() : as_Shared().value.clone();
    }
    auto clone() const -> StoreBinding { return is_Owned() ? Owned(store()) : Shared(store()); }
};
struct MemoryCacheOptions {
    usize max_entries { 16 };
    usize max_bytes { 16 * 1024 * 1024 };
    usize max_entry_bytes { 1024 * 1024 };
    usize max_metadata { 65536 };
};
struct MemoryCacheState;
class MemoryCache {
    Arc<MemoryCacheState>     state_;
    Arc<ResourceLease<empty>> registration_;
    MemoryCache(Arc<MemoryCacheState> state, Arc<ResourceLease<empty>> registration);

public:
    MemoryCache(MemoryCache&&) noexcept;
    ~MemoryCache();
    MemoryCache(const MemoryCache&) = delete;
    auto        operator=(MemoryCache&&) noexcept -> MemoryCache&;
    static auto make(ResourceContext, MemoryCacheOptions = {}) -> Result<MemoryCache>;
    auto        clone() const -> MemoryCache;
    auto        store() const -> Arc<rstd::dyn<CacheStore>>;
    auto        lookup(CacheKey, TransferContext) const -> coro<Result<CacheLookup>>;
    auto        begin_write(CacheWriteSpec, TransferContext) const -> coro<Result<CacheWriter>>;
    auto update(u64, u64, CacheMetadata, TransferContext) const -> coro<Result<CommitResult>>;
    auto invalidate(CacheTarget, TransferContext) const -> coro<Result<empty>>;
    void request_close() const;
    auto close() const -> coro<Result<empty>>;
};
} // namespace lifetchto::http
export namespace rstd
{
template<>
struct Impl<lifetchto::http::CacheStore, lifetchto::http::MemoryCache>
    : ImplBase<lifetchto::http::MemoryCache> {
    auto lookup(lifetchto::http::CacheKey key, lifetchto::TransferContext context) const {
        return this->self().lookup(rstd::move(key), rstd::move(context));
    }
    auto begin_write(lifetchto::http::CacheWriteSpec spec,
                     lifetchto::TransferContext      context) const {
        return this->self().begin_write(rstd::move(spec), rstd::move(context));
    }
    auto update(u64                            id,
                u64                            generation,
                lifetchto::http::CacheMetadata metadata,
                lifetchto::TransferContext     context) const {
        return this->self().update(id, generation, rstd::move(metadata), rstd::move(context));
    }
    auto invalidate(lifetchto::http::CacheTarget target, lifetchto::TransferContext context) const {
        return this->self().invalidate(rstd::move(target), rstd::move(context));
    }
    void request_close() const { this->self().request_close(); }
    auto close() const { return this->self().close(); }
};
} // namespace rstd
