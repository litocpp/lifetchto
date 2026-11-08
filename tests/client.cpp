#include <rstd/test/gtest.hpp>
#include "support.hpp"
import liserveto;
using namespace rstd::prelude;
using namespace rstd::literals;
using namespace lifetchto;
using namespace test_http;
using rstd::bytes::Bytes;
using rstd::async::coro;
using rstd::net::TcpListener;
using rstd::net::SocketAddr;
using rstd::time::Duration;

auto peer(TcpListener listener, usize requests) -> coro<empty> {
    auto accepted = co_await listener.accept();
    auto stream   = rstd::move(rstd::get<0>(accepted.unwrap()));
    for (usize i; i < requests; ++i) {
        auto bytes = co_await stream.recv();
        EXPECT_TRUE(bytes.is_ok());
        EXPECT_FALSE(bytes->is_empty());
        auto sent = co_await stream.send_all(Bytes::copy_from_slice(
            "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello"_str.as_bytes()));
        EXPECT_TRUE(sent.is_ok());
    }
    (void)co_await rstd::move(stream).close();
    (void)co_await rstd::move(listener).close();
    co_return empty {};
}
TEST(Http1Client, StreamsAndReusesConnection) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        auto listener = TcpListener::bind(SocketAddr::ipv4_loopback(u16())).unwrap();
        auto port     = listener.local_addr().unwrap().port();
        auto server   = runtime.handle().spawn(peer(rstd::move(listener), usize(2)));
        auto client   = TestClient::make(runtime.handle()).unwrap();
        for (int i = 0; i < 2; ++i) {
            auto request =
                TestRequest::make(rstd::format("http://127.0.0.1:{}/x", port).as_str()).unwrap();
            auto response = co_await client.send(rstd::move(request));
            EXPECT_TRUE(response.is_ok());
            if (response.is_err()) {
                (void)co_await client.close();
                co_return empty {};
            }
            EXPECT_EQ(response->head().status.value(), u16(200));
            auto body = response->take_body().unwrap();
            auto text = co_await body.text(usize(100));
            EXPECT_TRUE(text.is_ok());
            if (text.is_err()) {
                (void)co_await client.close();
                co_return empty {};
            }
            EXPECT_EQ(text->as_str(), "hello"_str);
        }
        EXPECT_TRUE((co_await client.close()).is_ok());
        EXPECT_TRUE((co_await rstd::move(server)).is_ok());
        co_return empty {};
    };
    runtime.block_on(task());
}

struct ResolverStats {
    usize calls {}, completed {};
    bool  started {}, released {};
};
struct ResolverGate {
    rstd::sync::Mutex<ResolverStats> stats { ResolverStats {} };
    rstd::sync::Condvar              changed;
};
struct BlockingResolver {
    rstd::async::RuntimeHandle    runtime;
    rstd::sync::Arc<ResolverGate> gate;
    auto resolve(String, u16 port) const -> coro<lifetchto::Result<Vec<SocketAddr>>> {
        {
            auto f = gate->stats.lock().unwrap();
            ++f->calls;
        }
        auto work   = runtime
                          .spawn_blocking([state = gate.clone(), port] {
                            {
                                auto f     = state->stats.lock().unwrap();
                                f->started = true;
                                state->changed.wait_while(f, [](const ResolverStats& stats) {
                                    return ! stats.released;
                                });
                                ++f->completed;
                            }
                            Vec<SocketAddr> addresses;
                            addresses.push(SocketAddr::ipv4_loopback(port));
                            return addresses;
                          })
                          .unwrap();
        auto result = co_await rstd::move(work);
        if (result.is_err()) co_return Err(lifetchto::Error::Closed());
        co_return Ok(rstd::move(result).unwrap());
    }
};
namespace rstd
{
template<>
struct Impl<lifetchto::http::Resolver, BlockingResolver> : ImplBase<BlockingResolver> {
    auto resolve(String host, u16 port) const {
        return this->self().resolve(rstd::move(host), port);
    }
};
} // namespace rstd
TEST(ClientResolver, OperatingSystemLookupOwnsAddressesAndErrors) {
    auto result = rstd::net::lookup_host("localhost"_str, u16(4242));
    ASSERT_TRUE(result.is_ok());
    EXPECT_FALSE(result->is_empty());
    for (auto address : *result) EXPECT_EQ(address.port(), u16(4242));
    auto numeric = rstd::net::lookup_host("127.0.0.1"_str, u16(8080)).unwrap();
    EXPECT_TRUE(numeric[usize()].is_loopback());
    EXPECT_EQ(numeric[usize()].port(), u16(8080));
    EXPECT_TRUE(rstd::net::lookup_host(""_str, u16(80)).is_err());
    auto invalid = rstd::net::lookup_host("host\0suffix"_str, u16(80));
    ASSERT_TRUE(invalid.is_err());
    EXPECT_FALSE(invalid.unwrap_err().message.is_empty());
}
TEST(ClientLifetime, ResolverJoinSurvivesTimeoutAndCanceledCloseWait) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        auto              gate = rstd::sync::Arc<ResolverGate>::make();
        TestClientOptions options;
        options.max_connections = usize(1);
        options.max_waiters     = usize();
        options.total_timeout   = Some(Duration::from_millis(u64(20)));
        options.resolver        = Some(rstd::sync::Arc<rstd::dyn<Resolver>>::make(
            BlockingResolver { runtime.handle(), gate.clone() }));
        auto client             = TestClient::make(runtime.handle(), rstd::move(options)).unwrap();
        auto first              = runtime.handle().spawn(
            client.send(TestRequest::make("http://blocked.test:9/"_str).unwrap()));
        bool started = false;
        for (int i = 0; i < 2000 && ! started; ++i) {
            {
                auto f  = gate->stats.lock().unwrap();
                started = f->started;
            }
            if (! started) co_await rstd::async::sleep(Duration::from_millis(u64(1)));
        }
        EXPECT_TRUE(started);
        co_await rstd::async::sleep(Duration::from_millis(u64(30)));
        auto early = co_await rstd::async::timeout(client.close().into_future(),
                                                   Duration::from_millis(u64(1)));
        EXPECT_TRUE(early.is_err());
        {
            auto f = gate->stats.lock().unwrap();
            EXPECT_EQ(f->completed, usize());
            f->released = true;
        }
        gate->changed.notify_all();
        auto result = (co_await rstd::move(first)).unwrap();
        EXPECT_TRUE(result.is_err());
        if (result.is_err()) EXPECT_TRUE(result.unwrap_err().is_Timeout());
        EXPECT_TRUE((co_await client.close()).is_ok());
        auto f = gate->stats.lock().unwrap();
        EXPECT_EQ(f->calls, usize(1));
        EXPECT_EQ(f->completed, usize(1));
        co_return empty {};
    };
    runtime.block_on(task());
}
TEST(ClientLifetime, EntryOwnsStateBeforeFirstPoll) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        auto pending = [&] {
            auto client = TestClient::make(runtime.handle()).unwrap();
            return client.send(TestRequest::make("http://127.0.0.1:9/"_str).unwrap());
        }();
        auto result = co_await rstd::move(pending);
        EXPECT_TRUE(result.is_err());
        if (result.is_err()) EXPECT_TRUE(result.unwrap_err().is_Canceled());
        co_return empty {};
    };
    runtime.block_on(task());
}

TEST(Http1Client, LiservetoInteroperability) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        auto  listener = TcpListener::bind(SocketAddr::ipv4_loopback(u16())).unwrap();
        auto  address  = listener.local_addr().unwrap();
        auto  shutdown = liserveto::Shutdown::make().unwrap();
        usize calls {};
        auto  endpoint =
            liserveto::Endpoint::make([&](liserveto::Request) -> coro<liserveto::ResponseResult> {
                ++calls;
                if (calls == usize(1))
                    co_return Ok(liserveto::Response::stream(liserveto::Body::from_bytes(
                        Bytes::copy_from_slice("chunked"_str.as_bytes()))));
                co_return Ok(
                    liserveto::Response::bytes(Bytes::copy_from_slice("fixed"_str.as_bytes())));
            });
        auto server = runtime.handle().spawn(
            liserveto::serve(rstd::move(listener),
                             rstd::sync::Arc<liserveto::Endpoint>::make(rstd::move(endpoint)),
                             shutdown.clone()));
        auto client = TestClient::make(runtime.handle()).unwrap();
        for (auto expected : array<ref<str>, 2> { "chunked"_str, "fixed"_str }) {
            auto req      = TestRequest::make(
                                rstd::format("http://127.0.0.1:{}/interop", address.port()).as_str())
                                .unwrap();
            auto response = (co_await client.send(rstd::move(req))).unwrap();
            auto body     = response.take_body().unwrap();
            EXPECT_EQ((co_await body.text(usize(100))).unwrap().as_str(), expected);
        }
        EXPECT_TRUE((co_await client.close()).is_ok());
        (void)shutdown.request_stop();
        auto report = (co_await rstd::move(server)).unwrap().unwrap();
        EXPECT_EQ(report.accepted, usize(1));
        EXPECT_EQ(report.failed, usize());
        EXPECT_EQ(calls, usize(2));
        co_return empty {};
    };
    runtime.block_on(task());
}

TEST(ClientLifetime, UploadOwnsInputBeforeFirstPoll) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto pending = [] {
        auto body = lifetchto::UploadBody::bytes(Bytes::copy_from_slice("owned"_str.as_bytes()));
        return body.next();
    }();
    auto part = runtime.block_on(rstd::move(pending)).unwrap().unwrap();
    EXPECT_EQ(rstd::str_::from_utf8(part.as_slice()).unwrap(), "owned"_str);
}
