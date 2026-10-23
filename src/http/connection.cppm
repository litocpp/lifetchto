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
struct Connection {
    Option<rstd::net::TcpStream> tcp;
#if RSTD_OS_UNIX
    Option<rstd::net::UnixStream> unix;
#endif
#if LITO_FEAT_TLS
    Option<licrypto::tls::Stream> tls;
#endif
    auto recv(usize size) -> coro<Result<Bytes>> {
#if LITO_FEAT_TLS
        if (tls.is_some()) return tls_result(tls->recv(size));
#endif
#if RSTD_OS_UNIX
        if (unix.is_some()) return io_result(unix->recv(size));
#endif
        return io_result(tcp->recv(size));
    }
    auto send(Bytes bytes) -> coro<Result<usize>> {
#if LITO_FEAT_TLS
        if (tls.is_some()) return tls_result(tls->send_all(rstd::move(bytes)));
#endif
#if RSTD_OS_UNIX
        if (unix.is_some()) return io_result(unix->send_all(rstd::move(bytes)));
#endif
        return io_result(tcp->send_all(rstd::move(bytes)));
    }
    auto close() -> coro<Result<empty>> {
#if LITO_FEAT_TLS
        if (tls.is_some()) return tls_result(rstd::move(tls.take().unwrap()).close());
#endif
#if RSTD_OS_UNIX
        if (unix.is_some()) return io_result(rstd::move(unix.take().unwrap()).close());
#endif
        return io_result(rstd::move(tcp.take().unwrap()).close());
    }
};
auto connect_deadline(const TransportOptions& options, const BodyState& body) -> Instant {
    auto result = Instant::now() + options.connect_timeout;
    if (body.deadline.is_some() && *body.deadline < result) result = *body.deadline;
    return result;
}
auto connect_to(Arc<NativeState>     client,
                Arc<Slot>            slot,
                const lihttpto::Url& url,
                Arc<BodyState>       body) -> coro<Result<Connection>> {
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
    auto                       origin = url.http_origin().unwrap();
    Vec<rstd::net::SocketAddr> addresses;
    if (slot->route.is_some()) {
        for (auto address : (*slot->route)->addresses()) addresses.push(rstd::move(address));
    } else if (auto numeric = url.numeric_address(); numeric.is_some())
        addresses.push(numeric.unwrap());
    else if (client->options.resolver.is_some()) {
        // Resolver futures remain owned until they complete, even after the caller times out.
        auto result =
            co_await (*client->options.resolver)->resolve(origin.host.clone(), origin.port);
        if (result.is_err()) co_return Err(rstd::move(result).unwrap_err());
        addresses = rstd::move(result).unwrap();
    } else {
        auto spawned =
            client->runtime.spawn_blocking([host = origin.host.clone(), port = origin.port] {
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
    Option<Error> last;
    for (auto address : addresses) {
        {
            auto f = body->fields.lock().unwrap();
            if (f->canceled) co_return Err(Error::Canceled());
        }
        if (Instant::now() >= deadline) co_return Err(Error::Timeout());
        auto prepared = rstd::net::TcpStream::prepare_connect(address);
        if (prepared.is_err()) {
            last = Some(Error::Connect(prepared.unwrap_err()));
            continue;
        }
        auto connecting = rstd::move(prepared).unwrap();
        auto finish     = Some(connecting.finish().into_future());
        auto connected  = co_await rstd::async::select(
            rstd::future::by_ref(*finish), connect_stop(body.clone(), deadline).into_future());
        if (connected.is_right()) {
            finish = None();
            (void)co_await rstd::move(connecting).close();
            co_return Err(rstd::move(connected).unwrap_right());
        }
        auto result = rstd::move(connected).unwrap_left();
        if (result.is_err()) {
            last = Some(Error::Connect(result.unwrap_err()));
            (void)co_await rstd::move(connecting).close();
            continue;
        }
        connection.tcp = Some(rstd::move(connecting).into_stream().unwrap());
        (void)connection.tcp->set_nodelay(true);
        break;
    }
    if (connection.tcp.is_none())
        co_return Err(last.is_some() ? last.take().unwrap() : Error::Capacity());
    if (origin.scheme.as_str() == "https"_str) {
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
        if (protocol.is_err() ||
            (! protocol->is_empty() && protocol->as_slice() != "http/1.1"_str.as_bytes())) {
            (void)co_await connection.close();
            co_return Err(
                Error::Unsupported(String::make("server negotiated unsupported ALPN"_str)));
        }
#else
        (void)co_await connection.close();
        co_return Err(Error::Unsupported(String::make("HTTPS requires feature tls"_str)));
#endif
    }
    co_return Ok(rstd::move(connection));
}
} // namespace lifetchto::http
