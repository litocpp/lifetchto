export module lifetchto.http:upgrade;
export import :duplex;
using namespace rstd::prelude;
export namespace lifetchto::http
{
struct UpgradeRequest {
    DuplexRequest            target;
    lihttpto::UpgradeRequest handshake;
    static auto              make(DuplexRequest             target,
                                  lihttpto::Method          method,
                                  lihttpto::UpgradeProtocol protocol,
                                  const lihttpto::Headers&  headers = {},
                                  usize                     limit   = usize(65536))
        -> rstd::Result<UpgradeRequest, lihttpto::UpgradeError> {
        auto origin = target.url.http_origin();
        if (origin.is_none() || target.url.userinfo().is_some())
            return Err(lihttpto::UpgradeError::InvalidRequest);
        auto handshake = lihttpto::UpgradeRequest::make(rstd::move(method),
                                                        target.url.request_target().as_str(),
                                                        origin->authority().as_str(),
                                                        rstd::move(protocol),
                                                        headers,
                                                        limit);
        if (handshake.is_err()) return Err(handshake.unwrap_err());
        return Ok(UpgradeRequest { rstd::move(target), rstd::move(handshake).unwrap() });
    }
};
struct UpgradeOptions {
    DuplexOptions duplex;
    Duration      handshake_timeout { Duration::from_secs(u64(10)) };
    usize         header_bytes { 65536 }, error_body_bytes { 65536 };
};
struct UpgradeFailure {
    Option<Error>                    transport;
    Option<lihttpto::HttpParseError> parse;
    Option<lihttpto::UpgradeError>   protocol;
    Option<lihttpto::ResponseHead>   response;
    Option<lihttpto::DecodeError>    body;
    Bytes                            error_body;
    bool                             body_complete {};
    Option<Arc<TransferReport>>      report;
};
struct UpgradeConnectionState;
class UpgradeConnection {
    Arc<UpgradeConnectionState> state_;
    static auto                 read_from(Arc<UpgradeConnectionState>) -> coro<Result<Bytes>>;

public:
    UpgradeConnection(DuplexConnection, lihttpto::ResponseHead, Bytes);
    UpgradeConnection(UpgradeConnection&&) noexcept;
    UpgradeConnection(const UpgradeConnection&) = delete;
    auto operator=(UpgradeConnection&&) noexcept -> UpgradeConnection&;
    ~UpgradeConnection();
    auto response() const -> const lihttpto::ResponseHead&;
    auto read() const -> coro<Result<Bytes>>;
    auto write(Bytes) const -> coro<Result<usize>>;
    auto close_input() const -> coro<Result<empty>>;
    auto can_close_input() const -> bool;
    auto finish() const -> Result<empty>;
    auto completion() const -> TransferControl;
    void cancel() const;
    auto close() const -> coro<Arc<TransferReport>>;
};
struct PendingUpgradeState;
class PendingUpgrade {
    Arc<PendingUpgradeState> state_;
    void                     abandon() const;
    static auto              receive(Arc<PendingUpgradeState>)
        -> coro<rstd::Result<UpgradeConnection, UpgradeFailure>>;

public:
    PendingUpgrade(PendingDuplex, lihttpto::UpgradeRequest, UpgradeOptions);
    PendingUpgrade(PendingUpgrade&&) noexcept;
    PendingUpgrade(const PendingUpgrade&) = delete;
    auto operator=(PendingUpgrade&&) noexcept -> PendingUpgrade&;
    ~PendingUpgrade();
    auto opened() const -> coro<rstd::Result<UpgradeConnection, UpgradeFailure>>;
    auto completion() const -> TransferControl;
    void cancel() const;
    auto close() const -> coro<Arc<TransferReport>>;
};
} // namespace lifetchto::http
