export module lifetchto.http:duplex;
export import :route;
using namespace rstd::prelude;
export namespace lifetchto::http
{
struct DuplexRequest {
    lihttpto::Url url;
    RouteChoice   route;
    auto          clone() const -> DuplexRequest { return { url.clone(), route.clone() }; }
};
class TotalTimeout {
    Option<Duration> duration_;
    explicit TotalTimeout(Option<Duration> duration): duration_(duration) {}

public:
    static auto disabled() -> TotalTimeout { return TotalTimeout(None()); }
    static auto after(Duration duration) -> TotalTimeout { return TotalTimeout(Some(duration)); }
    auto        duration() const -> Option<Duration> { return duration_; }
};
struct DuplexOptions {
    TransferOptions      transfer;
    Option<TotalTimeout> total_timeout;
};
struct ConnectionPlan {
    DuplexRequest   request;
    TransferOptions transfer;
};
struct PendingDuplexState;
class DuplexConnection {
    Transfer<DuplexStream> transfer_;
    DuplexStream           stream_;

public:
    DuplexConnection(Transfer<DuplexStream> transfer, DuplexStream stream)
        : transfer_(rstd::move(transfer)), stream_(rstd::move(stream)) {}
    DuplexConnection(DuplexConnection&&) noexcept                    = default;
    DuplexConnection(const DuplexConnection&)                        = delete;
    auto operator=(DuplexConnection&&) noexcept -> DuplexConnection& = default;
    auto next() const -> coro<Result<Option<Bytes>>> { return stream_.next(); }
    auto write(Bytes bytes) const -> coro<Result<usize>> {
        return stream_.write(rstd::move(bytes));
    }
    auto close_input() const -> coro<Result<empty>> { return stream_.close_input(); }
    auto can_close_input() const -> bool { return stream_.can_close_input(); }
    auto finish() const -> Result<empty> { return stream_.finish(); }
    auto completion() const -> TransferControl { return transfer_.control(); }
    void cancel() const { stream_.cancel(); }
    auto close() const -> coro<Arc<TransferReport>> { return stream_.close(); }
};
class PendingDuplex {
    Arc<PendingDuplexState> state_;
    void                    abandon() const;
    static auto             receive(Arc<PendingDuplexState>) -> coro<Result<DuplexConnection>>;

public:
    explicit PendingDuplex(Transfer<DuplexStream>);
    PendingDuplex(PendingDuplex&&) noexcept;
    PendingDuplex(const PendingDuplex&) = delete;
    auto operator=(PendingDuplex&&) noexcept -> PendingDuplex&;
    ~PendingDuplex();
    auto connected() const -> coro<Result<DuplexConnection>>;
    auto completion() const -> TransferControl;
    void cancel() const;
    auto close() const -> coro<Arc<TransferReport>>;
};
} // namespace lifetchto::http
