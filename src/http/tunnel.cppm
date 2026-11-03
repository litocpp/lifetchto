export module lifetchto.http:tunnel;
export import :options;
using namespace rstd::prelude;
export namespace lifetchto::http
{
struct TunnelPool;
struct TunnelStreamState;
class TunnelConnection {
    lihttpto::ResponseHead  head_;
    rstd::async::io::Stream stream_;

public:
    TunnelConnection(lihttpto::ResponseHead head, rstd::async::io::Stream stream)
        : head_(rstd::move(head)), stream_(rstd::move(stream)) {}
    TunnelConnection(TunnelConnection&&) noexcept                    = default;
    TunnelConnection(const TunnelConnection&)                        = delete;
    auto operator=(TunnelConnection&&) noexcept -> TunnelConnection& = default;
    auto head() const -> const lihttpto::ResponseHead& { return head_; }
    auto into_stream() && -> rstd::async::io::Stream { return rstd::move(stream_); }
};
class PendingTunnel {
    Arc<TunnelStreamState> state_;
    Arc<TunnelPool>        pool_;
    void                   abandon() const;

public:
    PendingTunnel(Arc<TunnelStreamState>, Arc<TunnelPool>);
    PendingTunnel(PendingTunnel&&) noexcept;
    PendingTunnel(const PendingTunnel&) = delete;
    auto operator=(PendingTunnel&&) noexcept -> PendingTunnel&;
    ~PendingTunnel();
    auto connected() const -> coro<Result<TunnelConnection>>;
    auto progress() const -> ConnectProgress;
    void request_stop() const;
    auto close() const -> coro<Result<empty>>;
};
class TunnelClient {
    Arc<TunnelPool>           pool_;
    Arc<ResourceLease<empty>> registration_;
    TunnelClient(Arc<TunnelPool>, Arc<ResourceLease<empty>>);
    void abandon() const;

public:
    TunnelClient(TunnelClient&&) noexcept;
    TunnelClient(const TunnelClient&) = delete;
    auto operator=(TunnelClient&&) noexcept -> TunnelClient&;
    ~TunnelClient();
    static auto make(ResourceContext  resources,
                     lihttpto::Url    proxy_url,
                     RouteChoice      route,
                     TransportOptions options = {}) -> Result<TunnelClient>;
    auto        clone() const -> TunnelClient;
    auto        open(String authority, Vec<lihttpto::h2::Field> fields = {}) const
        -> Result<PendingTunnel>;
    void request_close() const;
    auto close() const -> coro<Result<empty>>;
};
} // namespace lifetchto::http
