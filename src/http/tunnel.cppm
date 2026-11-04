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
    usize                   initial_payload_confirmed_ {};

public:
    TunnelConnection(lihttpto::ResponseHead  head,
                     rstd::async::io::Stream stream,
                     usize                   initial_payload_confirmed = {})
        : head_(rstd::move(head)),
          stream_(rstd::move(stream)),
          initial_payload_confirmed_(initial_payload_confirmed) {}
    TunnelConnection(TunnelConnection&&) noexcept                    = default;
    TunnelConnection(const TunnelConnection&)                        = delete;
    auto operator=(TunnelConnection&&) noexcept -> TunnelConnection& = default;
    auto head() const -> const lihttpto::ResponseHead& { return head_; }
    auto initial_payload_confirmed() const -> usize { return initial_payload_confirmed_; }
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
    auto initial_payload_progress() const -> Option<InitialPayloadProgress>;
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
    // Accepts at most upload_window bytes before the response; connected still requires 2xx.
    auto open(String authority, Vec<lihttpto::h2::Field> fields, Bytes initial_payload) const
        -> rstd::Result<PendingTunnel, Rejected<Bytes>>;
    void request_close() const;
    auto close() const -> coro<Result<empty>>;
    auto wait_closed() const -> coro<CleanupReport>;
    auto take_close_result() const -> Option<Result<empty>>;
};
} // namespace lifetchto::http
