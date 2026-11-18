export module lifetchto.core:duplex;
export import :bytes;
using namespace rstd::prelude;
namespace lifetchto
{
struct DuplexState;
struct DuplexAck;
export class DuplexWrite {
    Arc<DuplexAck>                ack_;
    rstd::sync::Weak<DuplexState> state_;
    Bytes                         bytes_;
    bool                          half_close_ {}, completed_ {};
    friend class DuplexChannel;
    friend auto duplex_write(Arc<DuplexState>, Bytes, bool) -> coro<Result<usize>>;
    DuplexWrite(Arc<DuplexAck>                ack,
                rstd::sync::Weak<DuplexState> state,
                Bytes                         bytes,
                bool                          half_close);

public:
    DuplexWrite(DuplexWrite&& other) noexcept;
    DuplexWrite(const DuplexWrite&) = delete;
    auto operator=(DuplexWrite&& other) noexcept -> DuplexWrite&;
    ~DuplexWrite();
    auto bytes() const -> const Bytes& { return bytes_; }
    auto is_half_close() const -> bool { return half_close_; }
    void complete(Result<usize> result);
};
export class DuplexStream {
    Arc<DuplexState> state_;
    friend class DuplexChannel;
    explicit DuplexStream(Arc<DuplexState> state);
    void abandon() const;

public:
    DuplexStream(DuplexStream&&) noexcept;
    DuplexStream(const DuplexStream&) = delete;
    auto operator=(DuplexStream&& other) noexcept -> DuplexStream&;
    ~DuplexStream();
    auto next() const -> coro<Result<Option<Bytes>>>;
    auto write(Bytes bytes) const -> coro<Result<usize>>;
    auto close_input() const -> coro<Result<empty>>;
    auto can_close_input() const -> bool;
    auto finish() const -> Result<empty>;
    auto take_write_error() const -> Option<Error>;
    auto completion() const -> TransferControl;
    void cancel() const;
    auto close() const -> coro<Arc<TransferReport>>;
};
export class DuplexChannel {
    Arc<DuplexState> state_;
    explicit DuplexChannel(Arc<DuplexState> state);

public:
    DuplexChannel(DuplexChannel&&) noexcept;
    auto operator=(DuplexChannel&&) noexcept -> DuplexChannel&;
    ~DuplexChannel();
    static auto make(TransferContext context, usize window = usize(65536), bool half_close = true)
        -> coro<Result<DuplexChannel>>;
    auto clone() const -> DuplexChannel;
    auto take_stream() const -> Result<DuplexStream>;
    auto available() const -> usize;
    auto receive(Bytes bytes) const -> coro<Result<empty>>;
    auto try_receive(Bytes bytes) const -> Result<empty>;
    void end_input() const;
    auto next_write() const -> coro<Result<DuplexWrite>>;
    auto try_write() const -> Result<Option<DuplexWrite>>;
    void request_stop(StopReason reason = StopReason::Canceled) const;
    void fail(Error error) const;
    auto cancellation() const -> Cancellation;
    auto wait_finished() const -> coro<Result<empty>>;
    auto failure() const -> Option<FailureReport>;
    void release_buffers() const;
};
} // namespace lifetchto
