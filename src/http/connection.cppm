module;
#include <rstd/macro.hpp>
module lifetchto.http:connection;
import :state;
using namespace rstd::prelude;
namespace lifetchto::http
{
template<class T>
auto io_result(rstd::async::coro<rstd::io::Result<T>> operation) -> coro<Result<T>> {
    auto result = co_await rstd::move(operation);
    if (result.is_err()) co_return Err(Error::Connect(result.unwrap_err()));
    co_return Ok(rstd::move(result).unwrap());
}
#if LITO_FEAT_TLS
template<class T>
auto tls_result(rstd::async::coro<licrypto::CryptoResult<T>> operation) -> coro<Result<T>> {
    auto result = co_await rstd::move(operation);
    if (result.is_err()) co_return Err(tls_error(rstd::move(result).unwrap_err()));
    co_return Ok(rstd::move(result).unwrap());
}
#endif
enum class ConnectionClose
{
    Protocol,
    SendOnly
};
struct Connection {
    bool                         http2 {};
    Bytes                        pending;
    Option<rstd::net::TcpStream> tcp;
#if RSTD_OS_UNIX
    Option<rstd::net::UnixStream> unix;
#endif
#if LITO_FEAT_TLS
    Option<licrypto::tls::Stream> tls;
#endif
    auto recv(usize size) -> coro<Result<Bytes>> {
        if (! pending.is_empty()) {
            auto count  = rstd::min(size, pending.len());
            auto result = pending.slice(usize(), count);
            pending.advance(count);
            return buffered(rstd::move(result));
        }
#if LITO_FEAT_TLS
        if (tls.is_some()) return tls_result(tls->recv(size));
#endif
#if RSTD_OS_UNIX
        if (unix.is_some()) return io_result(unix->recv(size));
#endif
        return io_result(tcp->recv(size));
    }
    static auto buffered(Bytes bytes) -> coro<Result<Bytes>> { co_return Ok(rstd::move(bytes)); }
    auto        send(Bytes bytes) -> coro<Result<usize>> {
#if LITO_FEAT_TLS
        if (tls.is_some()) return tls_result(tls->send_all(rstd::move(bytes)));
#endif
#if RSTD_OS_UNIX
        if (unix.is_some()) return io_result(unix->send_all(rstd::move(bytes)));
#endif
        return io_result(tcp->send_all(rstd::move(bytes)));
    }
    auto send_some(Bytes bytes) -> coro<Result<usize>> {
#if LITO_FEAT_TLS
        if (tls.is_some()) return tls_result(tls->send(rstd::move(bytes)));
#endif
#if RSTD_OS_UNIX
        if (unix.is_some()) return io_result(unix->send(rstd::move(bytes)));
#endif
        return io_result(tcp->send(rstd::move(bytes)));
    }
    auto send_body(Bytes bytes, Arc<BodyState> body, usize payload_offset, usize payload_length)
        -> coro<Result<usize>> {
        auto length = bytes.len();
        if (payload_offset > length || payload_length > length - payload_offset)
            co_return Err(Error::Contract("invalid HTTP payload span"_Str));
        usize completed;
        while (! bytes.is_empty()) {
            auto result = co_await send_some(bytes.clone());
            if (result.is_err()) co_return Err(rstd::move(result).unwrap_err());
            if (*result == usize() || *result > bytes.len())
                co_return Err(Error::Contract("invalid HTTP write completion"_Str));
            auto begin = rstd::cmp::max(completed, payload_offset);
            auto end   = rstd::cmp::min(completed + *result, payload_offset + payload_length);
            if (end > begin) body->sent_body(end - begin);
            completed += *result;
            bytes.advance(*result);
        }
        co_return Ok(length);
    }
    auto close(ConnectionClose behavior = ConnectionClose::Protocol) -> coro<Result<empty>> {
#if LITO_FEAT_TLS
        if (tls.is_some())
            return tls_result(rstd::move(tls.take().unwrap())
                                  .close(http2 || behavior == ConnectionClose::SendOnly
                                             ? licrypto::tls::CloseMode::SendOnly
                                             : licrypto::tls::CloseMode::WaitPeer));
#endif
#if RSTD_OS_UNIX
        if (unix.is_some()) return io_result(rstd::move(unix.take().unwrap()).close());
#endif
        return io_result(rstd::move(tcp.take().unwrap()).close());
    }
};
template<class T>
auto proxy_io(coro<Result<T>> operation, Arc<BodyState> body, Instant deadline) -> coro<Result<T>> {
    auto result =
        co_await rstd::async::select(rstd::move(operation).into_future(),
                                     connect_stop(rstd::move(body), deadline).into_future());
    if (result.is_right()) co_return Err(rstd::move(result).unwrap_right());
    co_return rstd::move(result).unwrap_left();
}
auto proxy_tunnel(Connection&             connection,
                  const lihttpto::Url&    target,
                  Arc<BodyState>          body,
                  Instant                 deadline,
                  const TransportOptions& options) -> coro<Result<empty>> {
    auto request = lihttpto::connect_request(target, options.head_limit);
    if (request.is_err())
        co_return Err(protocol_error("invalid CONNECT request"_Str, Some(request.unwrap_err())));
    auto sent =
        co_await proxy_io(connection.send(rstd::move(request).unwrap()), body.clone(), deadline);
    if (sent.is_err()) co_return Err(rstd::move(sent).unwrap_err());
    lihttpto::TunnelHeadDecoder head(options.head_limit);
    Bytes                       bytes;
    for (;;) {
        if (bytes.is_empty()) {
            auto received =
                co_await proxy_io(connection.recv(options.receive_window), body.clone(), deadline);
            if (received.is_err()) co_return Err(rstd::move(received).unwrap_err());
            bytes = rstd::move(received).unwrap();
            if (bytes.is_empty())
                co_return Err(protocol_error("proxy closed before CONNECT response"_Str,
                                             Some(lihttpto::DecodeError::Truncated)));
        }
        auto decoded = head.feed(bytes.as_slice());
        if (decoded.is_err())
            co_return Err(
                protocol_error("invalid CONNECT response"_Str, Some(decoded.unwrap_err())));
        if (decoded->is_none()) {
            bytes = {};
            continue;
        }
        auto value = decoded->take().unwrap();
        bytes.advance(value.input_consumed);
        if (value.disposition == lihttpto::TunnelDisposition::Informational) continue;
        if (value.disposition == lihttpto::TunnelDisposition::Established) {
            connection.pending = rstd::move(bytes);
            co_return Ok(empty {});
        }
        ProxyConnectFailure  failure { rstd::move(value.head) };
        lihttpto::BodyLimits limits;
        limits.data_bytes     = u64(options.head_limit.to_primitive());
        limits.metadata_bytes = options.head_limit;
        lihttpto::BodyDecoder decoder(value.framing, limits);
        Vec<u8>               collected;
        for (;;) {
            auto progress = decoder.feed(bytes.as_slice());
            if (progress.is_err()) {
                failure.framing = Some(progress.unwrap_err());
                break;
            }
            collected.extend_from_slice(progress->data);
            bytes.advance(progress->consumed);
            if (progress->status == lihttpto::DecodeStatus::Complete) {
                failure.body_complete = true;
                break;
            }
            if (! bytes.is_empty()) continue;
            auto received =
                co_await proxy_io(connection.recv(options.receive_window), body.clone(), deadline);
            if (received.is_err()) {
                failure.transport = Some(rstd::move(received).unwrap_err());
                break;
            }
            bytes = rstd::move(received).unwrap();
            if (bytes.is_empty()) {
                auto finished = decoder.finish();
                if (finished.is_err())
                    failure.framing = Some(finished.unwrap_err());
                else
                    failure.body_complete = true;
                break;
            }
        }
        failure.body = Bytes::copy_from_slice(collected.as_slice());
        co_return Err(
            Error::Protocol("proxy CONNECT rejected"_Str,
                            Some(Arc<rstd::dyn<rstd::error::Error>>::make(rstd::move(failure)))));
    }
}
auto connect_deadline(const TransportOptions& options, const BodyState& body) -> Instant {
    auto result = Instant::now() + options.connect_timeout;
    if (body.deadline.is_some() && *body.deadline < result) result = *body.deadline;
    return result;
}
auto close_dial(Arc<rstd::dyn<TcpDial>> dial, Arc<NativeState> client, Arc<BodyState> body)
    -> coro<empty> {
    auto result = co_await dial->close();
    if (result.is_err()) {
        {
            auto f = client->fields.lock().unwrap();
            if (f->close_error.is_none()) f->close_error = Some(result.unwrap_err().clone());
        }
        auto f = body->fields.lock().unwrap();
        if (f->cleanup_error.is_none()) f->cleanup_error = Some(rstd::move(result).unwrap_err());
    }
    co_return empty {};
}
auto connect_tcp(rstd::net::SocketAddr                address,
                 Option<Arc<rstd::dyn<TcpConnector>>> connector,
                 Arc<NativeState>                     client,
                 Arc<BodyState>                       body,
                 Instant deadline) -> coro<Result<rstd::net::TcpStream>> {
    if (connector.is_some()) {
        auto prepared = (*connector)->prepare(address);
        if (prepared.is_err()) co_return Err(rstd::move(prepared).unwrap_err());
        auto dial      = rstd::move(prepared).unwrap();
        auto finish    = Some(dial->finish().into_future());
        auto connected = co_await rstd::async::select(
            rstd::future::by_ref(*finish), connect_stop(body.clone(), deadline).into_future());
        finish = None();
        if (connected.is_right()) {
            co_await close_dial(dial.clone(), client.clone(), body.clone());
            co_return Err(rstd::move(connected).unwrap_right());
        }
        auto result = rstd::move(connected).unwrap_left();
        if (result.is_err()) {
            co_await close_dial(dial.clone(), client.clone(), body.clone());
            co_return Err(rstd::move(result).unwrap_err());
        }
        auto stream = dial->into_stream();
        if (stream.is_err()) co_await close_dial(dial.clone(), client.clone(), body.clone());
        co_return rstd::move(stream);
    }
    auto prepared = rstd::net::TcpStream::prepare_connect(address);
    if (prepared.is_err()) co_return Err(Error::Connect(prepared.unwrap_err()));
    auto connecting = rstd::move(prepared).unwrap();
    auto finish     = Some(connecting.finish().into_future());
    auto connected  = co_await rstd::async::select(
        rstd::future::by_ref(*finish), connect_stop(body.clone(), deadline).into_future());
    finish = None();
    if (connected.is_right()) {
        (void)co_await rstd::move(connecting).close();
        co_return Err(rstd::move(connected).unwrap_right());
    }
    auto result = rstd::move(connected).unwrap_left();
    if (result.is_err()) {
        (void)co_await rstd::move(connecting).close();
        co_return Err(Error::Connect(result.unwrap_err()));
    }
    auto stream = rstd::move(connecting).into_stream();
    if (stream.is_err()) co_return Err(Error::Connect(stream.unwrap_err()));
    co_return Ok(rstd::move(stream).unwrap());
}
auto connect_to(Arc<NativeState>     client,
                Arc<Slot>            slot,
                const lihttpto::Url& url,
                Arc<BodyState>       body,
                bool                 force_tunnel = false) -> coro<Result<Connection>> {
    auto deadline = connect_deadline(client->options, *body);
    {
        auto f = body->fields.lock().unwrap();
        if (f->canceled) co_return Err(Error::Canceled());
    }
    if (Instant::now() >= deadline) co_return Err(Error::Timeout());
    Connection connection;
    if (slot->route.is_some() && (*slot->route)->unix_path().is_some()) {
#if RSTD_OS_UNIX
        auto prepared =
            rstd::net::UnixStream::prepare_connect((*slot->route)->unix_path()->clone());
        if (prepared.is_err()) co_return Err(Error::Connect(prepared.unwrap_err()));
        auto connecting = rstd::move(prepared).unwrap();
        auto finish     = Some(connecting.finish().into_future());
        auto result     = co_await rstd::async::select(
            rstd::future::by_ref(*finish), connect_stop(body.clone(), deadline).into_future());
        if (result.is_right()) {
            finish = None();
            (void)co_await rstd::move(connecting).close();
            co_return Err(rstd::move(result).unwrap_right());
        }
        auto value = rstd::move(result).unwrap_left();
        if (value.is_err()) {
            (void)co_await rstd::move(connecting).close();
            co_return Err(Error::Connect(value.unwrap_err()));
        }
        connection.unix = Some(rstd::move(connecting).into_stream().unwrap());
        co_return Ok(rstd::move(connection));
#else
        co_return Err(Error::Unsupported(String::make("Unix sockets unavailable"_str)));
#endif
    }
    auto                       origin         = url.http_origin().unwrap();
    const auto&                destination    = slot->proxy.is_some() ? slot->proxy->url() : url;
    auto                       connect_origin = destination.http_origin().unwrap();
    Vec<rstd::net::SocketAddr> addresses;
    if (slot->route.is_some()) {
        for (auto address : (*slot->route)->addresses()) addresses.push(rstd::move(address));
    } else if (auto numeric = destination.numeric_address(); numeric.is_some())
        addresses.push(numeric.unwrap());
    else if (client->options.resolver.is_some()) {
        // Resolver futures remain owned until they complete, even after the caller times out.
        auto result = co_await (*client->options.resolver)
                          ->resolve(connect_origin.host.clone(), connect_origin.port);
        if (result.is_err()) co_return Err(rstd::move(result).unwrap_err());
        addresses = rstd::move(result).unwrap();
    } else {
        auto spawned = client->runtime.spawn_blocking(
            [host = connect_origin.host.clone(), port = connect_origin.port] {
                return rstd::net::lookup_host(host.as_str(), port);
            });
        if (spawned.is_err()) co_return Err(Error::Connect(spawned.unwrap_err()));
        auto joined = co_await rstd::move(spawned).unwrap();
        if (joined.is_err()) co_return Err(Error::Closed());
        auto result = rstd::move(joined).unwrap();
        if (result.is_err()) co_return Err(Error::Resolve(rstd::move(result).unwrap_err()));
        addresses = rstd::move(result).unwrap();
    }
    if (addresses.is_empty() || addresses.len() > usize(64)) co_return Err(Error::Capacity());
    Option<Arc<rstd::dyn<TcpConnector>>> connector;
    if (slot->route.is_some()) connector = (*slot->route)->connector().clone();
    Option<Error> last;
    for (auto address : addresses) {
        {
            auto f = body->fields.lock().unwrap();
            if (f->canceled) co_return Err(Error::Canceled());
        }
        if (Instant::now() >= deadline) co_return Err(Error::Timeout());
        auto result = co_await connect_tcp(
            address, connector.clone(), client.clone(), body.clone(), deadline);
        if (result.is_err()) {
            last = Some(rstd::move(result).unwrap_err());
            if (last->is_Canceled() || last->is_Timeout()) co_return Err(last.take().unwrap());
            continue;
        }
        connection.tcp  = Some(rstd::move(result).unwrap());
        auto configured = connection.tcp->set_nodelay(client->options.tcp.nodelay.unwrap_or(true));
        if (configured.is_ok() && client->options.tcp.keepalive.is_some())
            configured = connection.tcp->set_keepalive(*client->options.tcp.keepalive);
        if (configured.is_err()) {
            (void)co_await connection.close();
            co_return Err(Error::Connect(configured.unwrap_err()));
        }
        break;
    }
    if (connection.tcp.is_none())
        co_return Err(last.is_some() ? last.take().unwrap() : Error::Capacity());
    if (slot->proxy.is_some() && (force_tunnel || origin.scheme == "https"_str)) {
        auto tunnel =
            co_await proxy_tunnel(connection, url, body.clone(), deadline, client->options);
        if (tunnel.is_err()) {
            (void)co_await connection.close();
            co_return Err(rstd::move(tunnel).unwrap_err());
        }
    }
    if (origin.scheme.as_str() == "https"_str) {
        if (! connection.pending.is_empty()) {
            (void)co_await connection.close();
            co_return Err(protocol_error("unexpected data before tunneled TLS handshake"_Str));
        }
#if LITO_FEAT_TLS
        if (Instant::now() >= deadline) {
            (void)co_await connection.close();
            co_return Err(Error::Timeout());
        }
        auto                         context = client->options.tls->context();
        licrypto::tls::StreamOptions options;
        options.handshake_timeout = remaining(deadline);
        options.close_timeout     = client->options.tls_close_timeout;
        licrypto::tls::Connector connector(rstd::move(context), options);
        auto                     name = [&]() -> licrypto::CryptoResult<licrypto::tls::ServerName> {
            auto address = url.numeric_address();
            if (address.is_some()) {
                if (address->is_ipv4())
                    return Ok(licrypto::tls::ServerName::ip(address->ipv4_address().unwrap()));
                return Ok(licrypto::tls::ServerName::ip(address->ipv6_address().unwrap()));
            }
            return licrypto::tls::ServerName::dns(origin.host.as_str());
        }();
        if (name.is_err()) {
            (void)co_await connection.close();
            co_return Err(tls_error(rstd::move(name).unwrap_err()));
        }
        auto tls =
            co_await connector.connect(connection.tcp.take().unwrap(), rstd::move(name).unwrap());
        if (tls.is_err()) co_return Err(tls_error(rstd::move(tls).unwrap_err()));
        connection.tls = Some(rstd::move(tls).unwrap());
        auto protocol  = connection.tls->application_protocol();
        bool h2        = protocol.is_ok() && protocol->as_slice() == "h2"_str.as_bytes();
        bool h1 = protocol.is_ok() &&
                  (protocol->is_empty() || protocol->as_slice() == "http/1.1"_str.as_bytes());
#if ! LITO_FEAT_HTTP2
        h2 = false;
#endif
        if ((! h1 && ! h2) ||
            (h1 && client->options.tls->version_policy() == VersionPolicy::Http2) ||
            (h2 && client->options.tls->version_policy() == VersionPolicy::Http1)) {
            (void)co_await connection.close();
            co_return Err(Error::Unsupported("server negotiated unsupported ALPN"_Str));
        }
        connection.http2 = h2;
#else
        (void)co_await connection.close();
        co_return Err(Error::Unsupported(String::make("HTTPS requires feature tls"_str)));
#endif
    }
    co_return Ok(rstd::move(connection));
}
} // namespace lifetchto::http
