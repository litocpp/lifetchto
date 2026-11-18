module;
#include <rstd/macro.hpp>
module lifetchto.http:connection;
import :state;
using namespace rstd::prelude;

namespace lifetchto::http
{
enum class ConnectionClose
{
    Protocol,
    SendOnly
};
struct Connection {
    bool                            http2 {};
    Bytes                           pending;
    Option<rstd::async::io::Stream> tcp;
#if RSTD_OS_UNIX
    Option<rstd::net::UnixStream> unix;
#endif
#if LITO_FEAT_TLS
    Option<licrypto::tls::Stream> tls;
#endif
    auto recv(usize size) -> coro<Result<Bytes>>;
    auto send(Bytes bytes) -> coro<Result<usize>>;
    auto send_some(Bytes bytes) -> coro<Result<usize>>;
    auto flush() -> coro<Result<empty>>;
    auto send_confirmed(Bytes bytes) -> coro<Result<usize>>;
    auto send_body(Bytes bytes, Arc<BodyState> body, usize payload_offset, usize payload_length)
        -> coro<Result<usize>>;
    auto close(ConnectionClose behavior = ConnectionClose::Protocol) -> coro<Result<empty>>;
};

auto connect_to(Arc<NativeState>     client,
                Arc<Slot>            slot,
                const lihttpto::Url& url,
                Arc<BodyState>       body,
                bool                 force_tunnel = false) -> coro<Result<Connection>>;
} // namespace lifetchto::http
