#include <rstd/test/gtest.hpp>
#include <stdlib.h>
import rstd;
import lifetchto;
using namespace rstd::prelude;
using namespace rstd::literals;
using namespace lifetchto;
namespace
{
auto tunnel_options() -> http::TransportOptions {
    auto path = rstd::ffi::CStr::from_ptr(::getenv("LIFETCHTO_H2_CA")).to_str().unwrap();
    auto pem  = rstd::fs::read(rstd::path::PathBuf::from(path).as_path()).unwrap();
    http::TlsOptions trust;
    trust.use_system_roots = false;
    trust.ca_pem.push(Bytes::copy_from_slice(pem.as_slice()));
    http::TransportOptions options;
    options.tls =
        Some(http::TlsProfile::make(rstd::move(trust), http::VersionPolicy::Http2).unwrap());
    options.max_connections = options.max_connections_per_origin = usize(1);
    options.receive_window                                       = usize(4096);
    options.connect_timeout                                      = Duration::from_secs(u64(2));
    return options;
}
auto tunnel_client(Engine& engine) -> http::TunnelClient {
    auto port = u16(::atoi(::getenv("LIFETCHTO_H2_PORT")));
    auto url =
        lihttpto::Url::parse_http(rstd::format("https://origin.test:{}/", port).as_str()).unwrap();
    Vec<rstd::net::SocketAddr> addresses;
    addresses.push(rstd::net::SocketAddr::ipv4_loopback(port));
    http::RouteChoice route { Some(
        Arc<http::Route>::make(http::Route::tcp(url, rstd::move(addresses)).unwrap())) };
    return http::TunnelClient::make(
               engine.resources(), rstd::move(url), rstd::move(route), tunnel_options())
        .unwrap();
}
auto tunnel_receive(rstd::async::io::Stream& stream, usize count) -> coro<Bytes> {
    Vec<u8> bytes;
    while (bytes.len() < count) {
        auto part = (co_await stream.recv(count - bytes.len())).unwrap();
        if (part.is_empty()) break;
        bytes.extend_from_slice(part.as_slice());
    }
    co_return Bytes::from_vec(rstd::move(bytes));
}
} // namespace
TEST(H2Tunnel, MultiplexesAuthorityAndKeepsCancelAndHalfCloseIndependent) {
    if (! ::getenv("LIFETCHTO_H2_PORT")) GTEST_SKIP();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        auto engine = Engine::make(runtime.handle()).unwrap();
        auto client = tunnel_client(engine);
        auto one    = client.open("one.test:443"_Str).unwrap();
        auto two    = client.open("two.test:80"_Str).unwrap();
        auto first  = (co_await one.connected()).unwrap();
        auto second = (co_await two.connected()).unwrap();
        EXPECT_EQ(*first.head().headers.get_unique_text("x-connection"_str).unwrap(),
                  *second.head().headers.get_unique_text("x-connection"_str).unwrap());
        auto a = rstd::move(first).into_stream(), b = rstd::move(second).into_stream();
        EXPECT_EQ((co_await a.send_all(Bytes::copy_from_slice("hello"_bytes))).unwrap(), usize(5));
        EXPECT_EQ((co_await tunnel_receive(a, usize(5))).as_slice(), "hello"_bytes);
        EXPECT_TRUE((co_await a.shutdown_write()).is_ok());
        EXPECT_TRUE((co_await a.recv()).unwrap().is_empty());
        EXPECT_TRUE((co_await a.close()).is_ok());
        EXPECT_TRUE((co_await a.close()).is_ok());
        auto canceled_read =
            co_await rstd::async::timeout(b.recv().into_future(), Duration::from_millis(u64(1)));
        EXPECT_TRUE(canceled_read.is_err());
        EXPECT_EQ((co_await b.send_all(Bytes::copy_from_slice("survives"_bytes))).unwrap(),
                  usize(8));
        EXPECT_EQ((co_await tunnel_receive(b, usize(8))).as_slice(), "survives"_bytes);
        Vec<u8> block;
        block.resize(usize(1024), u8('x'));
        auto bytes = Bytes::from_vec(rstd::move(block));
        for (usize iteration; iteration < usize(24); ++iteration) {
            EXPECT_EQ((co_await b.send_all(bytes.clone())).unwrap(), bytes.len());
            EXPECT_EQ((co_await tunnel_receive(b, bytes.len())).as_slice(), bytes.as_slice());
        }
        auto canceled = client.open("cancel.test:443"_Str).unwrap();
        auto c        = rstd::move((co_await canceled.connected()).unwrap()).into_stream();
        EXPECT_TRUE((co_await c.close()).is_ok());
        EXPECT_EQ((co_await b.send_all(Bytes::copy_from_slice("yes"_bytes))).unwrap(), usize(3));
        EXPECT_EQ((co_await tunnel_receive(b, usize(3))).as_slice(), "yes"_bytes);
        EXPECT_TRUE((co_await b.close()).is_ok());
        EXPECT_TRUE((co_await client.close()).is_ok());
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(run());
}
TEST(H2Tunnel, RemoteEofDoesNotEndLocalWriteAndRejectsTypedProxyStatus) {
    if (! ::getenv("LIFETCHTO_H2_PORT")) GTEST_SKIP();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        auto engine     = Engine::make(runtime.handle()).unwrap();
        auto client     = tunnel_client(engine);
        auto pending    = client.open("half.test:443"_Str).unwrap();
        auto connection = (co_await pending.connected()).unwrap();
        auto stream     = rstd::move(connection).into_stream();
        EXPECT_EQ((co_await tunnel_receive(stream, usize(5))).as_slice(), "early"_bytes);
        EXPECT_TRUE((co_await stream.recv()).unwrap().is_empty());
        EXPECT_EQ((co_await stream.send_all(Bytes::copy_from_slice("after eof"_bytes))).unwrap(),
                  usize(9));
        EXPECT_TRUE((co_await stream.shutdown_write()).is_ok());
        EXPECT_TRUE((co_await stream.close()).is_ok());
        auto denied = client.open("deny.test:443"_Str).unwrap();
        auto result = co_await denied.connected();
        EXPECT_TRUE(result.is_err());
        if (result.is_ok()) co_return empty {};
        auto error = rstd::move(result).unwrap_err();
        EXPECT_TRUE(error.is_Extension());
        if (! error.is_Extension()) co_return empty {};
        auto cause = rstd::error::downcast_ref<http::ProxyConnectFailure>(
            error.as_Extension().cause.deref());
        EXPECT_TRUE(cause.is_some());
        if (cause.is_none()) co_return empty {};
        EXPECT_EQ((*cause)->response.status.value(), u16(407));
        EXPECT_TRUE((co_await denied.close()).is_ok());
        EXPECT_TRUE((co_await client.close()).is_ok());
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(run());
}
TEST(H2Tunnel, CancelBeforeFinalHeadReleasesStreamWithoutClosingSharedPool) {
    if (! ::getenv("LIFETCHTO_H2_PORT")) GTEST_SKIP();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        auto engine     = Engine::make(runtime.handle()).unwrap();
        auto client     = tunnel_client(engine);
        auto waiting    = client.open("stall.test:443"_Str).unwrap();
        auto ready      = client.open("ready.test:443"_Str).unwrap();
        auto connection = (co_await ready.connected()).unwrap();
        auto stream     = rstd::move(connection).into_stream();
        waiting.request_stop();
        EXPECT_TRUE((co_await waiting.close()).is_ok());
        auto result = co_await waiting.connected();
        EXPECT_TRUE(result.is_err());
        EXPECT_EQ((co_await stream.send_all(Bytes::copy_from_slice("still works"_bytes))).unwrap(),
                  usize(11));
        EXPECT_EQ((co_await tunnel_receive(stream, usize(11))).as_slice(), "still works"_bytes);
        EXPECT_TRUE((co_await stream.close()).is_ok());
        EXPECT_TRUE((co_await client.close()).is_ok());
        EXPECT_TRUE((co_await client.close()).is_ok());
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(run());
}
TEST(H2Tunnel, ReceiveOverwritesNonemptyBufferAndReusesStorage) {
    if (! ::getenv("LIFETCHTO_H2_PORT")) GTEST_SKIP();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        auto engine     = Engine::make(runtime.handle()).unwrap();
        auto client     = tunnel_client(engine);
        auto pending    = client.open("buffer.test:443"_Str).unwrap();
        auto connection = (co_await pending.connected()).unwrap();
        auto stream     = rstd::move(connection).into_stream();
        auto buffer     = rstd::bytes::BytesMut::with_capacity(usize(64));
        buffer.put_slice("stale-prefix"_bytes);
        auto* storage  = buffer.data();
        auto  capacity = buffer.capacity();
        for (auto payload : array<ref<str>, 3> { "hello"_str, "yes"_str, "123456789"_str }) {
            EXPECT_EQ(
                (co_await stream.send_all(Bytes::copy_from_slice(payload.as_bytes()))).unwrap(),
                payload.len());
            auto received = co_await stream.recv_into(rstd::move(buffer));
            EXPECT_EQ(received.result.unwrap(), received.buffer.len());
            EXPECT_EQ(received.buffer.as_slice(), payload.as_bytes());
            EXPECT_EQ(received.buffer.data(), storage);
            EXPECT_EQ(received.buffer.capacity(), capacity);
            buffer = rstd::move(received.buffer);
        }
        EXPECT_TRUE((co_await stream.shutdown_write()).is_ok());
        auto ended = co_await stream.recv_into(rstd::move(buffer));
        EXPECT_EQ(ended.result.unwrap(), usize());
        EXPECT_TRUE(ended.buffer.is_empty());
        EXPECT_EQ(ended.buffer.data(), storage);
        EXPECT_EQ(ended.buffer.capacity(), capacity);
        EXPECT_TRUE((co_await stream.close()).is_ok());
        EXPECT_TRUE((co_await client.close()).is_ok());
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(run());
}
