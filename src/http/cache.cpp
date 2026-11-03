module lifetchto.http;
import :cache;
using namespace rstd::prelude;
using namespace rstd::literals;
namespace lifetchto::http
{
struct CacheBodyOwner {
    BodyControl control;
    void        request_stop(StopReason reason) const { control.cancel(reason); }
    auto        release(ReleaseContext) const -> coro<Result<empty>> {
        (void)co_await control.close();
        co_return Ok(empty {});
    }
};
auto open_snapshot(Arc<rstd::dyn<CacheSnapshot>> snapshot, TransferContext context)
    -> coro<Result<ResponseBody>>;
auto CacheEntry::open(TransferContext context) const -> coro<Result<ResponseBody>> {
    return open_snapshot(snapshot_.clone(), rstd::move(context));
}
auto transaction_write(Arc<rstd::dyn<CacheTransaction>> tx, Bytes bytes, TransferContext context)
    -> coro<Result<empty>> {
    co_return co_await tx->write(rstd::move(bytes), rstd::move(context));
}
auto transaction_commit(Arc<rstd::dyn<CacheTransaction>> tx,
                        lihttpto::Headers                trailers,
                        TransferContext                  context) -> coro<Result<CommitResult>> {
    co_return co_await tx->commit(rstd::move(trailers), rstd::move(context));
}
auto transaction_abort(Arc<rstd::dyn<CacheTransaction>> tx) -> coro<Result<empty>> {
    co_return co_await tx->abort();
}
auto CacheWriter::write(Bytes bytes, TransferContext context) const -> coro<Result<empty>> {
    return transaction_write(transaction_.clone(), rstd::move(bytes), rstd::move(context));
}
auto CacheWriter::commit(lihttpto::Headers trailers, TransferContext context) const
    -> coro<Result<CommitResult>> {
    return transaction_commit(transaction_.clone(), rstd::move(trailers), rstd::move(context));
}
auto CacheWriter::abort() const -> coro<Result<empty>> {
    request_abort();
    return transaction_abort(transaction_.clone());
}
struct MemoryLedger {
    rstd::sync::Mutex<usize> used { usize() };
};
struct MemoryBlob {
    CacheMetadata       metadata;
    Vec<Bytes>          chunks;
    Vec<ResourcePermit> permits;
    Arc<MemoryLedger>   ledger;
    usize               weight;
    MemoryBlob(CacheMetadata       metadata,
               Vec<Bytes>          chunks,
               Vec<ResourcePermit> permits,
               Arc<MemoryLedger>   ledger,
               usize               weight)
        : metadata(rstd::move(metadata)),
          chunks(rstd::move(chunks)),
          permits(rstd::move(permits)),
          ledger(rstd::move(ledger)),
          weight(weight) {}
    ~MemoryBlob() {
        auto used = ledger->used.lock().unwrap();
        *used -= weight;
    }
};
struct MemoryRecord {
    CacheKey        key;
    u64             id;
    Arc<MemoryBlob> blob;
};
struct MemoryWriteState;
struct MemoryFields {
    Vec<MemoryRecord>                       entries;
    Vec<rstd::sync::Weak<MemoryWriteState>> writers;
    u64                                     generation { 1 }, next_id { 1 };
    bool                                    closed {};
};
struct MemoryCacheState {
    EngineHandle                    resources;
    MemoryCacheOptions              options;
    Arc<MemoryLedger>               ledger { Arc<MemoryLedger>::make() };
    rstd::sync::Mutex<MemoryFields> fields { MemoryFields {} };
    MemoryCacheState(ResourceContext resources, MemoryCacheOptions options)
        : resources(resources.handle()), options(options) {}
};
auto metadata_size(const CacheMetadata& meta) -> usize {
    usize size = usize(256) + meta.variant.len() + meta.request.url.as_ref().len() +
                 meta.request.method.as_ref().len();
    for (const auto& h : meta.head.headers)
        size += h.name.as_str().len() + h.value.as_slice().len() + usize(4);
    for (const auto& h : meta.request.headers)
        size += h.name.as_str().len() + h.value.as_slice().len() + usize(4);
    for (const auto& h : meta.trailers)
        size += h.name.as_str().len() + h.value.as_slice().len() + usize(4);
    for (const auto& name : meta.policy.fields.vary) size += name.len();
    if (meta.policy.fields.etag.is_some()) size += meta.policy.fields.etag->len();
    return size;
}
auto key_size(const CacheKey& key) -> usize {
    return key.target.len() + key.method.len() + key.partition.len();
}
auto reserve_cache(const Arc<MemoryCacheState>& state, MemoryFields& entries, usize size)
    -> Result<ResourcePermit> {
    for (;;) {
        bool enough;
        {
            auto used = state->ledger->used.lock().unwrap();
            enough    = size <= state->options.max_bytes - *used;
        }
        if (enough) break;
        if (entries.entries.is_empty()) return Err(Error::Capacity());
        entries.entries.remove(usize());
    }
    auto used      = state->ledger->used.lock().unwrap();
    auto resources = state->resources.resources();
    if (resources.is_err()) return Err(rstd::move(resources).unwrap_err());
    auto permit = resources->try_reserve(ResourceAmount { .bytes = size });
    if (permit.is_err()) return Err(rstd::move(permit).unwrap_err());
    *used += size;
    return rstd::move(permit);
}
struct MemoryWriteFields {
    Option<CacheWriteSpec> spec;
    Vec<Bytes>             chunks;
    Vec<ResourcePermit>    permits;
    usize                  bytes {}, weight {};
    bool                   ended {}, aborted {};
};
struct MemoryWriteState {
    Arc<MemoryCacheState>                cache;
    rstd::sync::Mutex<MemoryWriteFields> fields { MemoryWriteFields {} };
    explicit MemoryWriteState(Arc<MemoryCacheState> cache): cache(rstd::move(cache)) {}
    ~MemoryWriteState() {
        auto f = fields.lock().unwrap();
        if (f->weight != usize()) {
            auto used = cache->ledger->used.lock().unwrap();
            *used -= f->weight;
        }
    }
};
void abort_write(const Arc<MemoryWriteState>& state) {
    auto f = state->fields.lock().unwrap();
    if (f->ended) return;
    f->ended   = true;
    f->aborted = true;
    f->chunks.clear();
    f->permits.clear();
    f->spec = None();
    {
        auto used = state->cache->ledger->used.lock().unwrap();
        *used -= f->weight;
    }
    f->weight = usize();
}
void stop_cache(const Arc<MemoryCacheState>& state) {
    Vec<Arc<MemoryWriteState>> writers;
    {
        auto f    = state->fields.lock().unwrap();
        f->closed = true;
        f->entries.clear();
        for (const auto& weak : f->writers) {
            auto writer = weak.upgrade();
            if (writer) writers.push(rstd::move(writer));
        }
        f->writers.clear();
    }
    for (const auto& writer : writers) abort_write(writer);
}
struct MemoryReaderFields {
    Option<Arc<MemoryBlob>> blob;
    usize                   index {};
    bool                    canceled {}, ended {};
};
struct MemoryReader {
    Arc<rstd::sync::Mutex<MemoryReaderFields>> fields;
    explicit MemoryReader(Arc<MemoryBlob> blob)
        : fields(Arc<rstd::sync::Mutex<MemoryReaderFields>>::make(
              MemoryReaderFields { Some(rstd::move(blob)), usize(), false, false })) {}
    auto next() const -> coro<Result<Option<Bytes>>> {
        auto f = fields->lock().unwrap();
        if (f->canceled) co_return Err(Error::Canceled());
        if (f->ended) co_return Ok(None());
        auto& blob = **f->blob;
        if (f->index < blob.chunks.len()) co_return Ok(Some(blob.chunks[f->index++].clone()));
        f->ended = true;
        co_return Ok(None());
    }
    auto trailers() const -> Result<Option<lihttpto::Headers>> {
        auto f = fields->lock().unwrap();
        if (f->canceled) return Err(Error::Canceled());
        if (! f->ended || f->blob.is_none()) return Ok(None());
        return Ok(Some((*f->blob)->metadata.trailers.clone()));
    }
    void cancel() const {
        auto f = fields->lock().unwrap();
        if (! f->ended) f->canceled = true;
    }
    auto close() const -> coro<Result<empty>> {
        auto f = fields->lock().unwrap();
        if (! f->ended) f->canceled = true;
        f->blob = None();
        co_return Ok(empty {});
    }
};
struct MemorySnapshot {
    Arc<MemoryBlob> blob;
    auto            metadata() const -> const CacheMetadata& { return blob->metadata; }
    auto            open(TransferContext) const -> coro<Result<ResponseBody>>;
};
struct MemoryTransaction {
    Arc<MemoryWriteState> state;
    auto                  write(Bytes bytes, TransferContext context) const -> coro<Result<empty>> {
        if (context.cancellation().is_canceled()) co_return Err(Error::Canceled());
        auto parent = state->cache->fields.lock().unwrap();
        if (parent->closed) co_return Err(Error::Closed());
        auto f = state->fields.lock().unwrap();
        if (f->ended) co_return Err(Error::Closed());
        if (bytes.len() > state->cache->options.max_entry_bytes - f->bytes)
            co_return Err(Error::TooLarge());
        auto quota = reserve_cache(state->cache, *parent, bytes.len());
        if (quota.is_err()) co_return Err(rstd::move(quota).unwrap_err());
        f->weight += bytes.len();
        f->bytes += bytes.len();
        f->permits.push(rstd::move(quota).unwrap());
        f->chunks.push(rstd::move(bytes));
        co_return Ok(empty {});
    }
    auto commit(lihttpto::Headers trailers, TransferContext context) const
        -> coro<Result<CommitResult>> {
        if (context.cancellation().is_canceled()) {
            abort_write(state);
            co_return Err(Error::Canceled());
        }
        auto cache  = state->cache.clone();
        auto parent = cache->fields.lock().unwrap();
        auto f      = state->fields.lock().unwrap();
        if (f->ended) co_return Err(Error::Closed());
        if (parent->closed || parent->generation != f->spec->generation)
            co_return Ok(CommitResult::Conflict);
        f->spec->metadata.trailers = rstd::move(trailers);
        auto extra                 = metadata_size(f->spec->metadata) + key_size(f->spec->key);
        if (extra > cache->options.max_metadata) co_return Err(Error::TooLarge());
        auto initial = f->weight - f->bytes;
        if (extra > initial) {
            auto quota = reserve_cache(cache, *parent, extra - initial);
            if (quota.is_err()) co_return Err(rstd::move(quota).unwrap_err());
            f->weight += extra - initial;
            f->permits.push(rstd::move(quota).unwrap());
        }
        auto spec = f->spec.take().unwrap();
        auto blob = Arc<MemoryBlob>::make(rstd::move(spec.metadata),
                                          rstd::move(f->chunks),
                                          rstd::move(f->permits),
                                          cache->ledger.clone(),
                                          f->weight);
        f->weight = usize();
        f->ended  = true;
        for (usize i; i < parent->entries.len();) {
            const auto& record = parent->entries[i];
            bool        replace =
                (spec.replacing.is_some() && record.id == *spec.replacing) ||
                (record.key.matches(spec.key) &&
                 record.blob->metadata.variant.as_slice() == blob->metadata.variant.as_slice());
            if (replace)
                parent->entries.remove(i);
            else
                ++i;
        }
        while (parent->entries.len() >= cache->options.max_entries) parent->entries.remove(usize());
        parent->entries.push({ rstd::move(spec.key), parent->next_id++, rstd::move(blob) });
        ++parent->generation;
        co_return Ok(CommitResult::Published);
    }
    void request_abort() const { abort_write(state); }
    auto abort() const -> coro<Result<empty>> {
        abort_write(state);
        co_return Ok(empty {});
    }
};
struct MemoryOwner {
    Arc<MemoryCacheState> state;
    void                  request_stop(StopReason) const { stop_cache(state); }
    auto                  release(ReleaseContext) const -> coro<Result<empty>> {
        stop_cache(state);
        co_return Ok(empty {});
    }
};
} // namespace lifetchto::http
namespace rstd
{
template<>
struct Impl<lifetchto::http::ResponseSource, lifetchto::http::MemoryReader>
    : ImplBase<lifetchto::http::MemoryReader> {
    auto next() const { return this->self().next(); }
    auto trailers() const { return this->self().trailers(); }
    void cancel() const { this->self().cancel(); }
    auto close() const { return this->self().close(); }
};
template<>
struct Impl<lifetchto::http::CacheSnapshot, lifetchto::http::MemorySnapshot>
    : ImplBase<lifetchto::http::MemorySnapshot> {
    auto metadata() const -> const lifetchto::http::CacheMetadata& {
        return this->self().metadata();
    }
    auto open(lifetchto::TransferContext context) const {
        return this->self().open(rstd::move(context));
    }
};
template<>
struct Impl<lifetchto::http::CacheTransaction, lifetchto::http::MemoryTransaction>
    : ImplBase<lifetchto::http::MemoryTransaction> {
    auto write(lifetchto::Bytes bytes, lifetchto::TransferContext context) const {
        return this->self().write(rstd::move(bytes), rstd::move(context));
    }
    auto commit(lihttpto::Headers trailers, lifetchto::TransferContext context) const {
        return this->self().commit(rstd::move(trailers), rstd::move(context));
    }
    void request_abort() const { this->self().request_abort(); }
    auto abort() const { return this->self().abort(); }
};
template<>
struct Impl<lifetchto::ManagedResource, lifetchto::http::MemoryOwner>
    : ImplBase<lifetchto::http::MemoryOwner> {
    void request_stop(lifetchto::StopReason reason) const { this->self().request_stop(reason); }
    auto release(lifetchto::ReleaseContext context) const { return this->self().release(context); }
};
template<>
struct Impl<lifetchto::ManagedResource, lifetchto::http::CacheBodyOwner>
    : ImplBase<lifetchto::http::CacheBodyOwner> {
    void request_stop(lifetchto::StopReason reason) const { this->self().request_stop(reason); }
    auto release(lifetchto::ReleaseContext context) const { return this->self().release(context); }
};

} // namespace rstd
namespace lifetchto::http
{
auto open_snapshot(Arc<rstd::dyn<CacheSnapshot>> snapshot, TransferContext context)
    -> coro<Result<ResponseBody>> {
    auto body = co_await snapshot->open(context.clone());
    if (body.is_err()) co_return Err(rstd::move(body).unwrap_err());
    auto registered = context.resources().attach(
        empty {}, Arc<rstd::dyn<ManagedResource>>::make(CacheBodyOwner { body->completion() }));
    if (registered.is_err()) {
        auto rejected = rstd::move(registered).unwrap_err();
        auto closed   = co_await rejected.input.close();
        if (closed.is_err())
            context.resources().record_cleanup_error(rstd::move(closed).unwrap_err());
        co_return Err(rstd::move(rejected.error));
    }
    (void)rstd::move(registered).unwrap().into_scope();
    co_return rstd::move(body);
}
auto MemorySnapshot::open(TransferContext) const -> coro<Result<ResponseBody>> {
    co_return Ok(ResponseBody::from_source(
        Arc<rstd::dyn<ResponseSource>>::make(MemoryReader(blob.clone()))));
}
auto memory_lookup(Arc<MemoryCacheState> state, CacheKey key, TransferContext context)
    -> coro<Result<CacheLookup>> {
    if (context.cancellation().is_canceled()) co_return Err(Error::Canceled());
    auto f = state->fields.lock().unwrap();
    if (f->closed) co_return Err(Error::Closed());
    CacheLookup result { f->generation, {} };
    for (const auto& record : f->entries)
        if (record.key.matches(key))
            result.candidates.push(CacheEntry(
                record.id,
                Arc<rstd::dyn<CacheSnapshot>>::make(MemorySnapshot { record.blob.clone() })));
    co_return Ok(rstd::move(result));
}
auto memory_begin(Arc<MemoryCacheState> state, CacheWriteSpec spec, TransferContext context)
    -> coro<Result<CacheWriter>> {
    if (context.cancellation().is_canceled()) co_return Err(Error::Canceled());
    auto parent = state->fields.lock().unwrap();
    if (parent->closed) co_return Err(Error::Closed());
    for (usize i; i < parent->writers.len();)
        if (! parent->writers[i].upgrade())
            parent->writers.remove(i);
        else
            ++i;
    if (parent->writers.len() >= usize(256)) co_return Err(Error::Capacity());
    auto size = metadata_size(spec.metadata) + key_size(spec.key);
    if (size > state->options.max_metadata) co_return Err(Error::TooLarge());
    auto quota = reserve_cache(state, *parent, size);
    if (quota.is_err()) co_return Err(rstd::move(quota).unwrap_err());
    auto writer = Arc<MemoryWriteState>::make(state.clone());
    {
        auto f    = writer->fields.lock().unwrap();
        f->weight = size;
        f->permits.push(rstd::move(quota).unwrap());
        f->spec = Some(rstd::move(spec));
    }
    parent->writers.push(writer.downgrade());
    co_return Ok(CacheWriter(
        Arc<rstd::dyn<CacheTransaction>>::make(MemoryTransaction { rstd::move(writer) })));
}
auto memory_update(Arc<MemoryCacheState> state,
                   u64                   id,
                   u64                   generation,
                   CacheMetadata         metadata,
                   TransferContext       context) -> coro<Result<CommitResult>> {
    Option<Arc<MemoryBlob>> original;
    Option<CacheKey>        key;
    {
        auto f = state->fields.lock().unwrap();
        if (f->closed || f->generation != generation) co_return Ok(CommitResult::Conflict);
        for (const auto& entry : f->entries)
            if (entry.id == id) {
                original = Some(entry.blob.clone());
                key      = Some(entry.key.clone());
                break;
            }
    }
    if (original.is_none()) co_return Ok(CommitResult::Conflict);
    auto pending =
        co_await memory_begin(state.clone(),
                              { key.take().unwrap(), generation, rstd::move(metadata), Some(id) },
                              context.clone());
    if (pending.is_err()) co_return Err(rstd::move(pending).unwrap_err());
    auto writer = rstd::move(pending).unwrap();
    for (const auto& chunk : (*original)->chunks) {
        auto written = co_await writer.write(chunk.clone(), context.clone());
        if (written.is_err()) {
            (void)co_await writer.abort();
            co_return Err(rstd::move(written).unwrap_err());
        }
    }
    auto committed =
        co_await writer.commit((*original)->metadata.trailers.clone(), context.clone());
    if (committed.is_err() || *committed == CommitResult::Conflict) (void)co_await writer.abort();
    co_return rstd::move(committed);
}
auto memory_invalidate(Arc<MemoryCacheState> state, CacheTarget target, TransferContext context)
    -> coro<Result<empty>> {
    if (context.cancellation().is_canceled()) co_return Err(Error::Canceled());
    auto f = state->fields.lock().unwrap();
    if (f->closed) co_return Err(Error::Closed());
    ++f->generation;
    for (usize i; i < f->entries.len();) {
        auto& key = f->entries[i].key;
        if (key.target == target.target && key.partition == target.partition &&
            key.scope.matches(target.scope))
            f->entries.remove(i);
        else
            ++i;
    }
    co_return Ok(empty {});
}
auto MemoryCache::make(ResourceContext resources, MemoryCacheOptions options)
    -> Result<MemoryCache> {
    if (options.max_entries == usize() || options.max_entries > usize(65536) ||
        options.max_bytes == usize() || options.max_entry_bytes == usize() ||
        options.max_entry_bytes > options.max_bytes || options.max_metadata > options.max_bytes)
        return Err(Error::InvalidOptions(String::make("invalid memory cache capacity"_str)));
    auto state    = Arc<MemoryCacheState>::make(resources.clone(), options);
    auto attached = resources.attach(
        empty {}, Arc<rstd::dyn<ManagedResource>>::make(MemoryOwner { state.clone() }));
    if (attached.is_err()) return Err(rstd::move(attached).unwrap_err().error);
    return Ok(MemoryCache(rstd::move(state),
                          Arc<ResourceLease<empty>>::make(rstd::move(attached).unwrap())));
}
auto MemoryCache::store() const -> Arc<rstd::dyn<CacheStore>> {
    return Arc<rstd::dyn<CacheStore>>::make(clone());
}
auto MemoryCache::lookup(CacheKey key, TransferContext context) const -> coro<Result<CacheLookup>> {
    return memory_lookup(state_.clone(), rstd::move(key), rstd::move(context));
}
auto MemoryCache::begin_write(CacheWriteSpec spec, TransferContext context) const
    -> coro<Result<CacheWriter>> {
    return memory_begin(state_.clone(), rstd::move(spec), rstd::move(context));
}
auto MemoryCache::update(u64             id,
                         u64             generation,
                         CacheMetadata   metadata,
                         TransferContext context) const -> coro<Result<CommitResult>> {
    return memory_update(state_.clone(), id, generation, rstd::move(metadata), rstd::move(context));
}
auto MemoryCache::invalidate(CacheTarget target, TransferContext context) const
    -> coro<Result<empty>> {
    return memory_invalidate(state_.clone(), rstd::move(target), rstd::move(context));
}
void MemoryCache::request_close() const {
    stop_cache(state_);
}
auto MemoryCache::close() const -> coro<Result<empty>> {
    request_close();
    return registration_->release();
}
} // namespace lifetchto::http

namespace lifetchto::http
{
MemoryCache::MemoryCache(Arc<MemoryCacheState> state, Arc<ResourceLease<empty>> registration)
    : state_(rstd::move(state)), registration_(rstd::move(registration)) {
}
MemoryCache::MemoryCache(MemoryCache&&) noexcept                    = default;
MemoryCache::~MemoryCache()                                         = default;
auto MemoryCache::operator=(MemoryCache&&) noexcept -> MemoryCache& = default;
auto MemoryCache::clone() const -> MemoryCache {
    return MemoryCache(state_.clone(), registration_.clone());
}
} // namespace lifetchto::http
