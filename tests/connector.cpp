#include <rstd/test/gtest.hpp>
#include "support.hpp"
#include "tcp_connector.hpp"
using namespace rstd::prelude;
using namespace rstd::literals;
using namespace lifetchto;
using namespace test_http;
namespace
{
auto connector_peer(rstd::net::TcpListener listener) -> coro<empty> {
    for (usize connection; connection < usize(2); ++connection) {
        auto accepted = (co_await listener.accept()).unwrap();
        auto stream   = rstd::move(rstd::get<0>(accepted));
        auto requests = connection == usize() ? usize(2) : usize(1);
        for (usize request; request < requests; ++request) {
            auto bytes = (co_await stream.recv()).unwrap();
            auto text  = rstd::str_::from_utf8(bytes.as_slice()).unwrap();
            EXPECT_TRUE(text.find("upstream.test:8080"_str).is_some());
            EXPECT_TRUE((co_await stream.send_all(Bytes::copy_from_slice(
                             "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"_str.as_bytes())))
                            .is_ok());
        }
        (void)co_await rstd::move(stream).close();
    }
    (void)co_await rstd::move(listener).close();
    co_return empty {};
}
} // namespace
TEST(TcpConnector, LogicalAddressAndRouteIdentityRemainOwnedByHttp) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        auto listener =
            rstd::net::TcpListener::bind(rstd::net::SocketAddr::ipv4_loopback(u16())).unwrap();
        auto physical = listener.local_addr().unwrap();
        auto server   = runtime.handle().spawn(connector_peer(rstd::move(listener)));
        auto stats    = test_connector::Stats::make(test_connector::Counts {});
        auto url      = lihttpto::Url::parse_http("http://upstream.test:8080/query"_str).unwrap();
        auto destination = rstd::net::SocketAddr::ipv4_loopback(u16(1));
        auto connector   = Arc<rstd::dyn<http::TcpConnector>>::make(
            test_connector::Connector { physical, stats.clone() });
        auto make_route = [&] {
            Vec<rstd::net::SocketAddr> addresses;
            addresses.push(rstd::net::SocketAddr(destination));
            return Arc<Route>::make(
                Route::tcp(url, rstd::move(addresses), u64(), Some(connector.clone())).unwrap());
        };
        auto              first  = make_route();
        auto              second = make_route();
        TestClientOptions settings;
        settings.max_connections            = usize(1);
        settings.max_connections_per_origin = usize(1);
        auto client = TestClient::make(runtime.handle(), rstd::move(settings)).unwrap();
        for (usize index; index < usize(3); ++index) {
            ExchangeOptions options;
            options.route = Some(index < usize(2) ? first.clone() : second.clone());
            auto response = (co_await client.send(TestRequest::make(url.as_ref()).unwrap(),
                                                  rstd::move(options)))
                                .unwrap();
            auto body     = response.take_body().unwrap();
            EXPECT_EQ((co_await body.text(usize(10))).unwrap().as_str(), "ok"_str);
        }
        EXPECT_TRUE((co_await client.close()).is_ok());
        EXPECT_TRUE((co_await rstd::move(server)).is_ok());
        auto f = stats->lock().unwrap();
        EXPECT_EQ(f->prepared, usize(2));
        EXPECT_EQ(f->completed, usize(2));
        EXPECT_EQ(f->taken, usize(2));
        EXPECT_EQ(f->closes, usize());
        EXPECT_EQ(*f->destination, destination);
        co_return empty {};
    };
    runtime.block_on(task());
}
TEST(TcpConnector, FailedFinishAndHandoffAwaitClose) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        for (bool handoff : array<bool, 2> { false, true }) {
            auto listener =
                rstd::net::TcpListener::bind(rstd::net::SocketAddr::ipv4_loopback(u16())).unwrap();
            auto            stats = test_connector::Stats::make(test_connector::Counts {});
            auto            url   = lihttpto::Url::parse_http("http://upstream.test/"_str).unwrap();
            auto            route = test_connector::route(url,
                                                          listener.local_addr().unwrap(),
                                                          { listener.local_addr().unwrap(),
                                                            stats.clone(),
                                                            {},
                                                            Duration::from_millis(u64(10)),
                                                            ! handoff,
                                                            handoff });
            auto            client = TestClient::make(runtime.handle()).unwrap();
            ExchangeOptions options;
            options.route = Some(route.clone());
            auto result =
                co_await client.send(TestRequest::make(url.as_ref()).unwrap(), rstd::move(options));
            EXPECT_TRUE(result.is_err());
            if (result.is_err()) {
                auto error = rstd::move(result).unwrap_err();
                EXPECT_TRUE(error.is_Unsupported());
                EXPECT_TRUE(error.context().is_some());
                if (error.context().is_some()) {
                    auto phase =
                        rstd::error::downcast_ref<http::ConnectFailure>(error.context()->deref());
                    EXPECT_TRUE(phase.is_some());
                    if (phase.is_some()) EXPECT_EQ((*phase)->stage, http::ConnectStage::Dial);
                }
            }
            EXPECT_TRUE((co_await client.close()).is_ok());
            EXPECT_EQ(stats->lock().unwrap()->closes, usize(1));
            EXPECT_EQ(stats->lock().unwrap()->released, usize(1));
            EXPECT_EQ(stats->lock().unwrap()->closing_active, usize());
            (void)co_await rstd::move(listener).close();
        }
        co_return empty {};
    };
    runtime.block_on(task());
}
TEST(TcpConnector, TimeoutAndCanceledCloseWaitRetainDialCleanup) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        for (bool timeout : array<bool, 2> { true, false }) {
            auto              stats = test_connector::Stats::make(test_connector::Counts {});
            auto              url = lihttpto::Url::parse_http("http://upstream.test/"_str).unwrap();
            auto              address = rstd::net::SocketAddr::ipv4_loopback(u16(9));
            auto              route   = test_connector::route(url,
                                                              address,
                                                              { address,
                                                                stats.clone(),
                                                                Duration::from_secs(u64(1)),
                                                                Duration::from_millis(u64(80)) });
            TestClientOptions settings;
            settings.total_timeout =
                timeout ? Some(Duration::from_millis(u64(20))) : None<Duration>();
            auto client = TestClient::make(runtime.handle(), rstd::move(settings)).unwrap();
            ExchangeOptions options;
            options.route = Some(route.clone());
            auto sending  = runtime.handle().spawn(
                client.send(TestRequest::make(url.as_ref()).unwrap(), rstd::move(options)));
            co_await rstd::async::sleep(Duration::from_millis(u64(40)));
            auto early = co_await rstd::async::timeout(client.close().into_future(),
                                                       Duration::from_millis(u64(1)));
            EXPECT_TRUE(early.is_err());
            EXPECT_TRUE((co_await client.close()).is_ok());
            auto result = (co_await rstd::move(sending)).unwrap();
            EXPECT_TRUE(result.is_err());
            if (result.is_err()) {
                auto error = rstd::move(result).unwrap_err();
                EXPECT_TRUE(timeout ? error.is_Timeout() : error.is_Canceled());
            }
            auto f = stats->lock().unwrap();
            EXPECT_EQ(f->started, usize(1));
            EXPECT_EQ(f->completed, usize());
            EXPECT_EQ(f->active, usize());
            EXPECT_EQ(f->closes, usize(1));
            EXPECT_EQ(f->released, usize(1));
            EXPECT_EQ(f->closing_active, usize());
        }
        co_return empty {};
    };
    runtime.block_on(task());
}
TEST(TcpConnector, CleanupFailureRemainsVisibleToClientClose) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        auto stats   = test_connector::Stats::make(test_connector::Counts {});
        auto url     = lihttpto::Url::parse_http("http://upstream.test/"_str).unwrap();
        auto address = rstd::net::SocketAddr::ipv4_loopback(u16(9));
        auto route   = test_connector::route(
            url, address, { address, stats.clone(), {}, {}, true, false, true });
        auto            client = TestClient::make(runtime.handle()).unwrap();
        ExchangeOptions options;
        options.route = Some(route.clone());
        EXPECT_TRUE(
            (co_await client.send(TestRequest::make(url.as_ref()).unwrap(), rstd::move(options)))
                .is_err());
        EXPECT_TRUE((co_await client.close()).is_err());
        EXPECT_TRUE((co_await client.wait_closed()).is_err());
        EXPECT_EQ(stats->lock().unwrap()->released, usize(1));
        co_return empty {};
    };
    runtime.block_on(task());
}
TEST(TcpConnector, ExplicitProxyRejectsRouteBeforeDial) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        auto              stats   = test_connector::Stats::make(test_connector::Counts {});
        auto              url     = lihttpto::Url::parse_http("http://upstream.test/"_str).unwrap();
        auto              address = rstd::net::SocketAddr::ipv4_loopback(u16(9));
        auto              route   = test_connector::route(url, address, { address, stats.clone() });
        TestClientOptions settings;
        settings.proxy         = http::ProxyOptions::explicit_proxy(
                                     lihttpto::Url::parse_http("http://127.0.0.1:9"_str).unwrap())
                                     .unwrap();
        auto            client = TestClient::make(runtime.handle(), rstd::move(settings)).unwrap();
        ExchangeOptions options;
        options.route = Some(route.clone());
        auto result =
            co_await client.send(TestRequest::make(url.as_ref()).unwrap(), rstd::move(options));
        EXPECT_TRUE(result.is_err());
        if (result.is_err()) EXPECT_TRUE(result.unwrap_err().is_Unsupported());
        EXPECT_TRUE((co_await client.close()).is_ok());
        EXPECT_EQ(stats->lock().unwrap()->prepared, usize());
        co_return empty {};
    };
    runtime.block_on(task());
}
TEST(TcpConnector, EndpointRouteResolvesOnlyInsideOwnedDialAndReusesConnection) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        auto listener =
            rstd::net::TcpListener::bind(rstd::net::SocketAddr::ipv4_loopback(u16())).unwrap();
        auto address = listener.local_addr().unwrap();
        auto serve   = [](rstd::net::TcpListener listener) -> coro<empty> {
            auto accepted = (co_await listener.accept()).unwrap();
            auto stream   = rstd::move(rstd::get<0>(accepted));
            for (usize request; request < usize(2); ++request) {
                auto input = (co_await stream.recv()).unwrap();
                auto text  = rstd::str_::from_utf8(input.as_slice()).unwrap();
                EXPECT_TRUE(text.find("unresolvable.invalid:8080"_str).is_some());
                (co_await stream.send_all(Bytes::copy_from_slice(
                     "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"_bytes)))
                    .unwrap();
            }
            (void)co_await rstd::move(stream).close();
            (void)co_await rstd::move(listener).close();
            co_return empty {};
        };
        auto server = runtime.handle().spawn(serve(rstd::move(listener)));
        auto stats  = test_connector::Stats::make(test_connector::Counts {});
        auto url = lihttpto::Url::parse_http("http://unresolvable.invalid:8080/query"_str).unwrap();
        auto provider = Arc<rstd::dyn<http::EndpointConnector>>::make(
            test_connector::Endpoint { { address, stats.clone() } });
        auto route = Arc<Route>::make(Route::custom(url, u64(42), rstd::move(provider)).unwrap());
        EXPECT_TRUE(route->addresses().is_empty());
        auto client = TestClient::make(runtime.handle()).unwrap();
        for (usize request; request < usize(2); ++request) {
            ExchangeOptions options;
            options.route = Some(route.clone());
            auto response = (co_await client.send(TestRequest::make(url.as_ref()).unwrap(),
                                                  rstd::move(options)))
                                .unwrap();
            auto body     = response.take_body().unwrap();
            EXPECT_EQ((co_await body.text(usize(10))).unwrap().as_str(), "ok"_str);
        }
        EXPECT_TRUE((co_await client.close()).is_ok());
        EXPECT_TRUE((co_await rstd::move(server)).is_ok());
        EXPECT_EQ(stats->lock().unwrap()->prepared, usize(1));
        co_return empty {};
    };
    runtime.block_on(run());
}
