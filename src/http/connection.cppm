module;
#include <rstd/macro.hpp>
module lifetchto.http:connection;
import :state;
using rstd::time::Duration;
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
auto close_tls(licrypto::tls::Stream stream, licrypto::tls::CloseMode mode) -> coro<Result<empty>> {
    Vec<Error> errors;
    auto       result = co_await stream.close(mode);
    if (result.is_err()) errors.push(tls_error(rstd::move(result).unwrap_err()));
    for (;;) {
        auto error = stream.take_error();
        if (error.is_none()) break;
        errors.push(tls_error(error.take().unwrap()));
    }
    auto cleanup = stream.take_close_error();
    if (cleanup.is_some()) errors.push(tls_error(cleanup.take().unwrap()));
    co_return collect_errors(rstd::move(errors));
}
#endif
enum class ConnectionClose
{
    Protocol,
    SendOnly
};
void record_io_cleanup(const Arc<BodyState>& body, rstd::io::Result<empty> result) {
    if (result.is_err()) body->record_cleanup(Err(Error::Connect(rstd::move(result).unwrap_err())));
}
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
    auto flush() -> coro<Result<empty>> {
#if LITO_FEAT_TLS
        if (tls.is_some()) co_return co_await tls_result(tls->flush());
#endif
#if RSTD_OS_UNIX
        if (unix.is_some()) co_return Ok(empty {});
#endif
        co_return co_await io_result(tcp->flush());
    }
    auto send_confirmed(Bytes bytes) -> coro<Result<usize>> {
        auto sent = co_await send_some(rstd::move(bytes));
        if (sent.is_err()) co_return Err(rstd::move(sent).unwrap_err());
        // TLS accepting plaintext does not confirm its encrypted transport output.
        auto flushed = co_await flush();
        if (flushed.is_err()) co_return Err(rstd::move(flushed).unwrap_err());
        co_return rstd::move(sent);
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
            return close_tls(tls.take().unwrap(),
                             http2 || behavior == ConnectionClose::SendOnly
                                 ? licrypto::tls::CloseMode::SendOnly
                                 : licrypto::tls::CloseMode::WaitPeer);
#endif
#if RSTD_OS_UNIX
        if (unix.is_some()) return io_result(rstd::move(unix.take().unwrap()).close());
#endif
        return io_result(tcp.take().unwrap().close());
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
                  const ProxyEndpoint&    proxy,
                  Arc<BodyState>          body,
                  Instant                 deadline,
                  const TransportOptions& options) -> coro<Result<empty>> {
    lihttpto::Headers fields;
    auto              authorization = proxy.authorization();
    if (authorization.is_some()) {
        auto header = fields.set("Proxy-Authorization"_str, authorization->as_str());
        if (header.is_err())
            co_return Err(Error::Protocol(
                "invalid proxy authorization"_Str,
                Some(Box<rstd::dyn<rstd::error::Error>>::make(rstd::move(header).unwrap_err()))));
    }
    auto request = lihttpto::connect_request(target, fields, options.head_limit);
    if (request.is_err())
        co_return Err(protocol_error("invalid CONNECT request"_Str, Some(request.unwrap_err())));
    auto sent =
        co_await proxy_io(connection.send(rstd::move(request).unwrap()), body.clone(), deadline);
    if (sent.is_err()) co_return Err(rstd::move(sent).unwrap_err());
    auto flushed = co_await proxy_io(connection.flush(), body.clone(), deadline);
    if (flushed.is_err()) co_return Err(rstd::move(flushed).unwrap_err());
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
                            Some(Box<rstd::dyn<rstd::error::Error>>::make(rstd::move(failure)))));
    }
}
auto resolve_endpoint(Arc<NativeState> client, String host, u16 port)
    -> coro<Result<Vec<rstd::net::SocketAddr>>> {
    Vec<rstd::net::SocketAddr> addresses;
    auto                       ipv4 = rstd::net::Ipv4Addr::parse(host.as_str());
    if (ipv4.is_ok()) {
        addresses.push(rstd::net::SocketAddr::ipv4(*ipv4, port));
        co_return Ok(rstd::move(addresses));
    }
    auto ipv6 = rstd::net::Ipv6Addr::parse(host.as_str());
    if (ipv6.is_ok()) {
        addresses.push(rstd::net::SocketAddr::ipv6(*ipv6, port, u32(), u32()));
        co_return Ok(rstd::move(addresses));
    }
    if (client->options.resolver.is_some())
        co_return co_await (*client->options.resolver)->resolve(rstd::move(host), port);
    auto spawned = client->runtime.spawn_blocking([host = rstd::move(host), port] {
        return rstd::net::lookup_host(host.as_str(), port);
    });
    if (spawned.is_err()) co_return Err(Error::Connect(rstd::move(spawned).unwrap_err()));
    auto joined = co_await rstd::move(spawned).unwrap();
    if (joined.is_err()) co_return Err(Error::Closed());
    auto result = rstd::move(joined).unwrap();
    if (result.is_err()) co_return Err(Error::Resolve(rstd::move(result).unwrap_err()));
    co_return Ok(rstd::move(result).unwrap());
}
auto proxy_read_exact(Connection& connection, usize length, Arc<BodyState> body, Instant deadline)
    -> coro<Result<Vec<u8>>> {
    Vec<u8> bytes;
    while (bytes.len() < length) {
        auto received =
            co_await proxy_io(connection.recv(length - bytes.len()), body.clone(), deadline);
        if (received.is_err()) co_return Err(rstd::move(received).unwrap_err());
        if (received->is_empty())
            co_return Err(protocol_error("proxy closed during SOCKS5 handshake"_Str,
                                         Some(lihttpto::DecodeError::Truncated)));
        bytes.extend_from_slice(received->as_slice());
    }
    co_return Ok(rstd::move(bytes));
}
auto proxy_send(Connection& connection, Vec<u8> bytes, Arc<BodyState> body, Instant deadline)
    -> coro<Result<empty>> {
    auto sent = co_await proxy_io(
        connection.send(Bytes::copy_from_slice(bytes.as_slice())), body.clone(), deadline);
    if (sent.is_err()) co_return Err(rstd::move(sent).unwrap_err());
    if (*sent != bytes.len())
        co_return Err(Error::Contract("incomplete proxy handshake write"_Str));
    co_return co_await proxy_io(connection.flush(), rstd::move(body), deadline);
}
auto socks5_tunnel(Connection&          connection,
                   const lihttpto::Url& target,
                   const ProxyEndpoint& proxy,
                   Arc<NativeState>     client,
                   Arc<BodyState>       body,
                   Instant              deadline) -> coro<Result<empty>> {
    auto    username = proxy.username();
    auto    password = proxy.password();
    Vec<u8> greeting;
    greeting.push(u8(5));
    greeting.push(u8(1));
    greeting.push(username.is_some() ? u8(2) : u8());
    rstd_co_try(co_await proxy_send(connection, rstd::move(greeting), body.clone(), deadline));
    auto method =
        rstd_co_try(co_await proxy_read_exact(connection, usize(2), body.clone(), deadline));
    auto method_bytes = method.as_slice();
    auto expected     = username.is_some() ? u8(2) : u8();
    if (method_bytes[usize()] != u8(5) || method_bytes[usize(1)] != expected)
        co_return Err(protocol_error("SOCKS5 proxy rejected the authentication method"_Str));
    if (username.is_some()) {
        if (username->len() > usize(255) || password.len() > usize(255))
            co_return Err(Error::InvalidOptions("invalid SOCKS5 credential length"_Str));
        Vec<u8> authentication;
        authentication.push(u8(1));
        authentication.push(u8(username->len().to_primitive()));
        authentication.extend_from_slice(*username);
        authentication.push(u8(password.len().to_primitive()));
        authentication.extend_from_slice(password);
        rstd_co_try(
            co_await proxy_send(connection, rstd::move(authentication), body.clone(), deadline));
        auto reply =
            rstd_co_try(co_await proxy_read_exact(connection, usize(2), body.clone(), deadline));
        auto bytes = reply.as_slice();
        if (bytes[usize()] != u8(1) || bytes[usize(1)] != u8())
            co_return Err(protocol_error("SOCKS5 proxy authentication failed"_Str));
    }
    auto origin  = target.http_origin().unwrap();
    auto address = target.numeric_address();
    if (address.is_none() && proxy.type() == ProxyType::Socks5) {
        auto resolved = rstd_co_try(co_await resolve_endpoint(
            client.clone(), String::make(*target.host_name()), origin.port));
        if (resolved.is_empty() || resolved.len() > usize(64)) co_return Err(Error::Capacity());
        address = Some(rstd::net::SocketAddr(resolved.as_slice()[usize()]));
    }
    Vec<u8> request;
    request.push(u8(5));
    request.push(u8(1));
    request.push(u8());
    if (address.is_some()) {
        request.push(address->is_ipv4() ? u8(1) : u8(4));
        auto length = address->is_ipv4() ? usize(4) : usize(16);
        for (usize i {}; i < length; ++i) request.push(address->octet(i));
    } else {
        auto host = target.host_name().unwrap().as_bytes();
        if (host.is_empty() || host.len() > usize(255))
            co_return Err(Error::InvalidOptions("SOCKS5 target hostname is too long"_Str));
        request.push(u8(3));
        request.push(u8(host.len().to_primitive()));
        request.extend_from_slice(host);
    }
    request.push(u8(origin.port.to_primitive() >> 8));
    request.push(u8(origin.port.to_primitive() & 255));
    rstd_co_try(co_await proxy_send(connection, rstd::move(request), body.clone(), deadline));
    auto reply =
        rstd_co_try(co_await proxy_read_exact(connection, usize(4), body.clone(), deadline));
    auto bytes = reply.as_slice();
    if (bytes[usize()] != u8(5) || bytes[usize(2)] != u8())
        co_return Err(protocol_error("invalid SOCKS5 CONNECT response"_Str));
    if (bytes[usize(1)] != u8())
        co_return Err(
            protocol_error(rstd::format("SOCKS5 CONNECT failed with status {}", bytes[usize(1)])));
    usize length;
    switch (bytes[usize(3)].to_primitive()) {
    case 1: length = usize(4); break;
    case 4: length = usize(16); break;
    case 3: {
        auto size =
            rstd_co_try(co_await proxy_read_exact(connection, usize(1), body.clone(), deadline));
        length = usize(size.as_slice()[usize()].to_primitive());
        if (length == usize()) co_return Err(protocol_error("empty SOCKS5 bound hostname"_Str));
        break;
    }
    default: co_return Err(protocol_error("invalid SOCKS5 bound address type"_Str));
    }
    (void)rstd_co_try(
        co_await proxy_read_exact(connection, length + usize(2), body.clone(), deadline));
    co_return Ok(empty {});
}
#if LITO_FEAT_TLS
auto secure_connection(Connection&          connection,
                       const lihttpto::Url& url,
                       const TlsProfile&    profile,
                       Arc<BodyState>       body,
                       Instant              deadline,
                       Duration             close_timeout) -> coro<Result<empty>> {
    if (! connection.pending.is_empty()) {
        body->record_cleanup(co_await connection.close());
        co_return Err(protocol_error("unexpected data before tunneled TLS handshake"_Str));
    }
    bool canceled;
    {
        auto fields = body->fields.lock().unwrap();
        canceled    = fields->canceled;
    }
    if (canceled || Instant::now() >= deadline) {
        body->record_cleanup(co_await connection.close());
        co_return Err(canceled ? Error::Canceled() : Error::Timeout());
    }
    auto name = [&]() -> licrypto::CryptoResult<licrypto::tls::ServerName> {
        auto address = url.numeric_address();
        if (address.is_some()) {
            if (address->is_ipv4())
                return Ok(licrypto::tls::ServerName::ip(address->ipv4_address().unwrap()));
            return Ok(licrypto::tls::ServerName::ip(address->ipv6_address().unwrap()));
        }
        return licrypto::tls::ServerName::dns(*url.host_name());
    }();
    if (name.is_err()) {
        body->record_cleanup(co_await connection.close());
        co_return Err(tls_error(rstd::move(name).unwrap_err()));
    }
    licrypto::tls::StreamOptions options;
    options.handshake_timeout = remaining(deadline);
    options.close_timeout     = close_timeout;
    licrypto::tls::Connector connector(profile.context(), options);
    auto transport = connection.tls.is_some()
                         ? rstd::move(connection.tls.take().unwrap()).into_stream()
                         : connection.tcp.take().unwrap();
    auto tls       = co_await connector.connect(rstd::move(transport), rstd::move(name).unwrap());
    if (tls.is_err()) co_return Err(tls_error(rstd::move(tls).unwrap_err()));
    connection.tls = Some(rstd::move(tls).unwrap());
    {
        auto fields = body->fields.lock().unwrap();
        canceled    = fields->canceled;
    }
    if (canceled || Instant::now() >= deadline) {
        body->record_cleanup(co_await connection.close());
        co_return Err(canceled ? Error::Canceled() : Error::Timeout());
    }
    auto protocol = connection.tls->application_protocol();
    if (protocol.is_err()) {
        body->record_cleanup(co_await connection.close());
        co_return Err(tls_error(rstd::move(protocol).unwrap_err()));
    }
    bool h2 = protocol->as_slice() == "h2"_str.as_bytes();
    bool h1 = protocol->is_empty() || protocol->as_slice() == "http/1.1"_str.as_bytes();
#if ! LITO_FEAT_HTTP2
    h2 = false;
#endif
    if ((! h1 && ! h2) || (h1 && profile.version_policy() == VersionPolicy::Http2) ||
        (h2 && profile.version_policy() == VersionPolicy::Http1)) {
        body->record_cleanup(co_await connection.close());
        co_return Err(Error::Unsupported("server negotiated unsupported ALPN"_Str));
    }
    connection.http2 = h2;
    co_return Ok(empty {});
}
#endif
auto connect_deadline(const TransportOptions& options, const BodyState& body) -> Instant {
    auto result = Instant::now() + options.connect_timeout;
    if (body.deadline.is_some() && *body.deadline < result) result = *body.deadline;
    return result;
}
auto close_dial(Arc<rstd::dyn<TcpDial>> dial, Arc<NativeState> client, Arc<BodyState> body)
    -> coro<empty> {
    auto result = co_await dial->close();
    if (result.is_err()) {
        auto f = body->fields.lock().unwrap();
        f->cleanup_errors.push(rstd::move(result).unwrap_err());
    }
    co_return empty {};
}
auto connect_tcp(rstd::net::SocketAddr                address,
                 Option<Arc<rstd::dyn<TcpConnector>>> connector,
                 Arc<NativeState>                     client,
                 Arc<BodyState>                       body,
                 Instant deadline) -> coro<Result<rstd::async::io::Stream>> {
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
        record_io_cleanup(body, co_await rstd::move(connecting).close());
        co_return Err(rstd::move(connected).unwrap_right());
    }
    auto result = rstd::move(connected).unwrap_left();
    if (result.is_err()) {
        record_io_cleanup(body, co_await rstd::move(connecting).close());
        co_return Err(Error::Connect(result.unwrap_err()));
    }
    auto stream = rstd::move(connecting).into_stream();
    if (stream.is_err()) co_return Err(Error::Connect(stream.unwrap_err()));
    auto configured = stream->set_nodelay(client->options.tcp.nodelay.unwrap_or(true));
    if (configured.is_ok() && client->options.tcp.keepalive.is_some())
        configured = stream->set_keepalive(*client->options.tcp.keepalive);
    if (configured.is_err()) {
        record_io_cleanup(body, co_await rstd::move(*stream).close());
        co_return Err(Error::Connect(configured.unwrap_err()));
    }
    auto adapted = rstd::net::as_stream(rstd::move(*stream));
    if (adapted.is_err()) co_return Err(Error::Connect(adapted.unwrap_err()));
    co_return Ok(rstd::move(*adapted));
}
auto connect_native(Arc<NativeState>     client,
                    Arc<Slot>            slot,
                    const lihttpto::Url& url,
                    Arc<BodyState>       body,
                    bool                 force_tunnel = false) -> coro<Result<Connection>> {
    body->set_connect_stage(ConnectStage::Dial);
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
            record_io_cleanup(body, co_await rstd::move(connecting).close());
            co_return Err(rstd::move(result).unwrap_right());
        }
        auto value = rstd::move(result).unwrap_left();
        if (value.is_err()) {
            record_io_cleanup(body, co_await rstd::move(connecting).close());
            co_return Err(Error::Connect(value.unwrap_err()));
        }
        connection.unix = Some(rstd::move(connecting).into_stream().unwrap());
        co_return Ok(rstd::move(connection));
#else
        co_return Err(Error::Unsupported(String::make("Unix sockets unavailable"_str)));
#endif
    }
    auto origin = url.http_origin().unwrap();
    auto connect_host =
        slot->proxy.is_some() ? slot->proxy->host_name() : String::make(*url.host_name());
    auto connect_port = slot->proxy.is_some() ? slot->proxy->port() : origin.port;
    Vec<rstd::net::SocketAddr> addresses;
    if (slot->route.is_some() && (*slot->route)->endpoint_connector().is_some()) {
        auto prepared = (*(*slot->route)->endpoint_connector())->prepare();
        if (prepared.is_err()) co_return Err(rstd::move(prepared).unwrap_err());
        auto dial      = rstd::move(*prepared);
        auto finishing = Some(dial->finish().into_future());
        auto connected = co_await rstd::async::select(
            rstd::future::by_ref(*finishing), connect_stop(body.clone(), deadline).into_future());
        finishing = None();
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
        if (stream.is_err()) {
            co_await close_dial(dial.clone(), client.clone(), body.clone());
            co_return Err(rstd::move(stream).unwrap_err());
        }
        connection.tcp = Some(rstd::move(*stream));
    } else if (slot->route.is_some()) {
        for (auto address : (*slot->route)->addresses()) addresses.push(rstd::move(address));
    } else {
        auto resolved =
            co_await resolve_endpoint(client.clone(), rstd::move(connect_host), connect_port);
        if (resolved.is_err()) co_return Err(rstd::move(resolved).unwrap_err());
        addresses = rstd::move(resolved).unwrap();
    }
    if ((addresses.is_empty() && connection.tcp.is_none()) || addresses.len() > usize(64))
        co_return Err(Error::Capacity());
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
        connection.tcp = Some(rstd::move(result).unwrap());
        break;
    }
    if (connection.tcp.is_none())
        co_return Err(last.is_some() ? last.take().unwrap() : Error::Capacity());
    if (slot->proxy.is_some()) {
        body->set_connect_stage(ConnectStage::ProxyConnect);
        Result<empty> tunnel = Ok(empty {});
        switch (slot->proxy->type()) {
        case ProxyType::Http:
            if (force_tunnel || origin.scheme == "https"_str)
                tunnel = co_await proxy_tunnel(
                    connection, url, *slot->proxy, body.clone(), deadline, client->options);
            break;
        case ProxyType::Https: {
#if LITO_FEAT_TLS
            if (client->options.proxy_tls.is_none()) {
                body->record_cleanup(co_await connection.close());
                co_return Err(Error::InvalidOptions("HTTPS proxy requires a TLS profile"_Str));
            }
            auto secured = co_await secure_connection(connection,
                                                      slot->proxy->url(),
                                                      *client->options.proxy_tls,
                                                      body.clone(),
                                                      deadline,
                                                      client->options.tls_close_timeout);
            if (secured.is_err()) co_return Err(rstd::move(secured).unwrap_err());
            if (force_tunnel || origin.scheme == "https"_str)
                tunnel = co_await proxy_tunnel(
                    connection, url, *slot->proxy, body.clone(), deadline, client->options);
#else
            body->record_cleanup(co_await connection.close());
            co_return Err(Error::Unsupported("HTTPS proxy requires feature tls"_Str));
#endif
            break;
        }
        case ProxyType::Socks5:
        case ProxyType::Socks5h:
            tunnel = co_await socks5_tunnel(
                connection, url, *slot->proxy, client.clone(), body.clone(), deadline);
            break;
        case ProxyType::Socks4:
        case ProxyType::Socks4a:
            body->record_cleanup(co_await connection.close());
            co_return Err(Error::Unsupported("SOCKS4 proxy is unsupported"_Str));
        }
        if (tunnel.is_err()) {
            body->record_cleanup(co_await connection.close());
            co_return Err(rstd::move(tunnel).unwrap_err());
        }
    }
    if (origin.scheme.as_str() == "https"_str) {
        body->set_connect_stage(ConnectStage::Tls);
#if LITO_FEAT_TLS
        auto secured = co_await secure_connection(connection,
                                                  url,
                                                  *client->options.tls,
                                                  body.clone(),
                                                  deadline,
                                                  client->options.tls_close_timeout);
        if (secured.is_err()) co_return Err(rstd::move(secured).unwrap_err());
#else
        body->record_cleanup(co_await connection.close());
        co_return Err(Error::Unsupported("HTTPS requires feature tls"_Str));
#endif
    }
    co_return Ok(rstd::move(connection));
}
auto connect_to(Arc<NativeState>     client,
                Arc<Slot>            slot,
                const lihttpto::Url& url,
                Arc<BodyState>       body,
                bool                 force_tunnel = false) -> coro<Result<Connection>> {
    auto progress = body.clone();
    auto result   = co_await connect_native(
        rstd::move(client), rstd::move(slot), url, rstd::move(body), force_tunnel);
    if (result.is_err())
        co_return Err(connect_failure(rstd::move(result).unwrap_err(),
                                      ConnectProgress { progress->connect_stage() }));
    co_return rstd::move(result);
}
} // namespace lifetchto::http
