export module lifetchto.core:bytes;
export import :engine;
using namespace rstd::prelude;
using namespace rstd::literals;
namespace lifetchto
{
export struct ByteSource {
    template<class Self, class = void>
    struct Api {
        using Trait = ByteSource;
        auto next(usize window) const -> coro<Result<Option<Bytes>>> {
            return rstd::trait_call<0>(this, window);
        }
        void request_stop(StopReason reason) const { rstd::trait_call<1>(this, reason); }
        auto release(ReleaseContext context) const -> coro<Result<rstd::empty>> {
            return rstd::trait_call<2>(this, context);
        }
    };
    template<class T>
    using Funcs = rstd::TraitFuncs<&T::next, &T::request_stop, &T::release>;
};
template<class S>
struct StoredByteSource {
    Arc<S> source;
    auto   next(usize maximum) const -> coro<Result<Option<Bytes>>> {
        auto result = [&] {
            if constexpr (requires { source->next(maximum); })
                return source->next(maximum);
            else
                return source->next();
        }();
        auto part = co_await rstd::move(result);
        if (part.is_ok() && part->is_some() && (**part).len() > maximum)
            co_return Err(Error::TooLarge());
        co_return rstd::move(part);
    }
    void request_stop(StopReason reason) const {
        if constexpr (requires { source->request_stop(reason); })
            source->request_stop(reason);
        else if constexpr (requires { source->cancel(); })
            source->cancel();
    }
    auto release(ReleaseContext context) const -> coro<Result<rstd::empty>> {
        if constexpr (requires { source->release(context); })
            co_return co_await source->release(context);
        else if constexpr (requires { source->close(); })
            co_return co_await source->close();
        else
            co_return Ok(empty {});
    }
};
struct UploadSourceState;
export class UploadBody {
    Option<Bytes>                  bytes_;
    Option<Arc<UploadSourceState>> source_;
    Option<u64>                    length_ { Some(u64()) };

public:
    UploadBody();
    UploadBody(UploadBody&& other) noexcept;
    UploadBody(const UploadBody&) = delete;
    auto operator=(UploadBody&& other) noexcept -> UploadBody&;
    ~UploadBody();
    static auto bytes(Bytes bytes) -> UploadBody;
    static auto source(Arc<rstd::dyn<ByteSource>> source, Option<u64> length = {}) -> UploadBody;
    template<class S>
    static auto from_source(S source, Option<u64> length = {}) -> UploadBody;
    auto        length() const -> Option<u64> { return length_; }
    auto        next(usize maximum = usize(65536)) -> coro<Result<Option<Bytes>>>;
    void        cancel() const;
    void        request_close() const { cancel(); }
    auto        close() const -> coro<Result<rstd::empty>>;
    auto        wait_closed() const -> coro<CleanupReport>;
    auto        take_close_result() const -> Option<Result<empty>>;
};
export struct UploadMetadata {
    Option<u64> length { Some(u64()) };
};
export struct UploadFactory {
    template<class Self, class = void>
    struct Api {
        using Trait = UploadFactory;
        auto open(TransferContext context) const -> coro<Result<UploadBody>> {
            return rstd::trait_call<0>(this, rstd::move(context));
        }
        void request_stop(StopReason reason) const { rstd::trait_call<1>(this, reason); }
        auto release(ReleaseContext context) const -> coro<Result<rstd::empty>> {
            return rstd::trait_call<2>(this, context);
        }
    };
    template<class T>
    using Funcs = rstd::TraitFuncs<&T::open, &T::request_stop, &T::release>;
};
struct UploadPlanState;
export class UploadPlan {
    Arc<UploadPlanState> state_;

public:
    UploadPlan();
    UploadPlan(UploadPlan&&) noexcept;
    UploadPlan(const UploadPlan&) = delete;
    ~UploadPlan();
    auto        operator=(UploadPlan&& other) noexcept -> UploadPlan&;
    static auto empty() -> UploadPlan { return UploadPlan {}; }
    static auto bytes(Bytes bytes) -> UploadPlan;
    static auto once(UploadBody body) -> UploadPlan;
    static auto repeatable(UploadMetadata metadata, Arc<rstd::dyn<UploadFactory>> factory)
        -> UploadPlan;
    auto metadata() const -> UploadMetadata;
    auto length() const -> Option<u64>;
    auto replayable() const -> bool;
    auto open(TransferContext context) const -> coro<Result<UploadBody>>;
    void request_close() const;
    auto close() const -> coro<Result<rstd::empty>>;
    auto wait_closed() const -> coro<CleanupReport>;
    auto take_close_result() const -> Option<Result<rstd::empty>>;
};
} // namespace lifetchto
export namespace rstd
{
template<class S>
struct Impl<lifetchto::ByteSource, lifetchto::StoredByteSource<S>>
    : ImplBase<lifetchto::StoredByteSource<S>> {
    auto next(usize maximum) const { return this->self().next(maximum); }
    void request_stop(lifetchto::StopReason reason) const { this->self().request_stop(reason); }
    auto release(lifetchto::ReleaseContext context) const { return this->self().release(context); }
};
} // namespace rstd
namespace lifetchto
{
template<class S>
auto UploadBody::from_source(S source, Option<u64> length) -> UploadBody {
    return UploadBody::source(
        Arc<rstd::dyn<ByteSource>>::make(StoredByteSource<S> { Arc<S>::make(rstd::move(source)) }),
        length);
}
} // namespace lifetchto
namespace lifetchto
{
struct ByteQueueState;
export class ByteQueue {
    Arc<ByteQueueState> state_;

public:
    explicit ByteQueue(Arc<ByteQueueState> state);
    ByteQueue(ByteQueue&&) noexcept;
    ~ByteQueue();
    auto operator=(ByteQueue&&) noexcept -> ByteQueue&;

    static auto make(ResourceContext context, usize capacity) -> coro<Result<ByteQueue>>;
    auto        clone() const -> ByteQueue;
    auto        available() const -> usize;
    auto        changed() const -> EventWait;
    auto        readable() const -> Result<bool>;
    auto        wait_readable() const -> coro<Result<empty>>;
    auto        wait_writable() const -> coro<Result<empty>>;
    auto        try_send(Bytes bytes) const -> Result<empty>;
    auto        send(Bytes bytes) const -> coro<Result<empty>>;
    auto        next(usize maximum = usize(65536)) const -> coro<Result<Option<Bytes>>>;
    auto        try_next(usize maximum = usize(65536)) const -> Result<Option<Option<Bytes>>>;
    void        finish() const;
    void        fail(Error error) const;
    void        release() const;
};
} // namespace lifetchto
