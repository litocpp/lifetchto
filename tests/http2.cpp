#include <rstd/test/gtest.hpp>
#include <stdlib.h>
#if defined(__linux__)
#include <dirent.h>
#endif
#include "support.hpp"
using namespace rstd::prelude;
using namespace rstd::literals;
using namespace lifetchto;
using namespace test_http;
namespace
{
using http::VersionPolicy;
#if defined(__linux__)
auto h2_descriptors() -> usize {
    auto  directory = ::opendir("/proc/self/fd");
    usize count;
    while (::readdir(directory)) ++count;
    ::closedir(directory);
    return count;
}
#endif
auto h2_port(const char* name = "LIFETCHTO_H2_PORT") -> u16 {
    return u16(::atoi(::getenv(name)));
}
auto h2_url(ref<str> path, const char* port = "LIFETCHTO_H2_PORT") -> String {
    return rstd::format("https://origin.test:{}{}", h2_port(port), path);
}
auto h2_options(VersionPolicy version = VersionPolicy::Automatic) -> TestClientOptions {
    auto path = rstd::ffi::CStr::from_ptr(::getenv("LIFETCHTO_H2_CA")).to_str().unwrap();
    auto pem  = rstd::fs::read(rstd::path::PathBuf::from(path).as_path()).unwrap();
    licrypto::tls::Trust trust;
    trust.use_system_roots = false;
    trust.ca_pem.push(Bytes::copy_from_slice(pem.as_slice()));
    TestClientOptions options;
    options.tls             = Some(TlsProfile::make(rstd::move(trust), version).unwrap());
    options.max_connections = usize(1);
    options.max_connections_per_origin = usize(1);
    options.receive_window             = usize(4096);
    options.total_timeout              = Some(Duration::from_secs(u64(3)));
    return options;
}
auto h2_route(const char* port = "LIFETCHTO_H2_PORT") -> Arc<Route> {
    auto url = lihttpto::Url::parse_http(h2_url("/"_str, port).as_str()).unwrap();
    Vec<rstd::net::SocketAddr> addresses;
    addresses.push(rstd::net::SocketAddr::ipv4_loopback(h2_port(port)));
    return Arc<Route>::make(Route::tcp(url, rstd::move(addresses)).unwrap());
}
auto h2_get(TestClient& client, const Arc<Route>& route, ref<str> path)
    -> coro<lifetchto::Result<TestResponse>> {
    ExchangeOptions options;
    options.route = Some(route.clone());
    return client.send(TestRequest::make(h2_url(path).as_str()).unwrap(), rstd::move(options));
}
auto h2_text(TestResponse& response) -> coro<lifetchto::Result<String>> {
    auto body = response.take_body().unwrap();
    co_return co_await body.text(usize(1024 * 1024));
}
struct H2Upload {
    using Error = lifetchto::Error;
    usize remaining { 512 * 1024 };
    auto  next() -> coro<lifetchto::Result<Option<Bytes>>> {
        co_await rstd::async::sleep(Duration::from_millis(u64(1)));
        if (remaining == usize()) co_return Ok(None());
        auto count = rstd::cmp::min(remaining, usize(1024));
        remaining -= count;
        Vec<u8> bytes;
        bytes.resize(count, u8('x'));
        co_return Ok(Some(Bytes::from_vec(rstd::move(bytes))));
    }
};
} // namespace
TEST(H2Peer, StreamingUploadTrailersAndDynamicHeaders) {
    if (! ::getenv("LIFETCHTO_H2_PORT")) GTEST_SKIP();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        auto client = TestClient::make(runtime.handle(), h2_options()).unwrap();
        auto route  = h2_route();
        for (auto path : array<ref<str>, 5> {
                 "/ok"_str, "/informational"_str, "/trailers"_str, "/204"_str, "/304"_str }) {
            auto response = (co_await h2_get(client, route, path)).unwrap();
            EXPECT_EQ(response.head().version->major(), u8(2));
            EXPECT_EQ(response.head().headers.get_all("x-dup"_str).len(), usize(2));
            auto body = response.take_body().unwrap();
            auto text = (co_await body.text(usize(100))).unwrap();
            EXPECT_EQ(text.as_str(),
                      path == "/204"_str || path == "/304"_str ? ""_str : "hello"_str);
            if (path == "/trailers"_str)
                EXPECT_EQ(*body.trailers().unwrap()->get_unique_text("x-end"_str).unwrap(),
                          "yes"_str);
        }
        auto request   = TestRequest::make(h2_url("/echo"_str).as_str(), "POST"_str).unwrap();
        request.body() = TestUploadBody::from_source(H2Upload {});
        lihttpto::Headers trailers;
        trailers.add("x-upload-end"_str, "yes"_str).unwrap();
        request.body().set_trailers(rstd::move(trailers));
        request.parts().headers.add("Expect"_str, "100-continue"_str).unwrap();
        ExchangeOptions options;
        options.route = Some(route.clone());
        auto response = (co_await client.send(rstd::move(request), rstd::move(options))).unwrap();
        auto body     = response.take_body().unwrap();
        EXPECT_EQ((co_await body.bytes(usize(1024 * 1024))).unwrap().len(), usize(512 * 1024));
        EXPECT_TRUE((co_await client.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(task());
}
TEST(H2Peer, UnreadStreamAndCancellationLeaveSiblingUsable) {
    if (! ::getenv("LIFETCHTO_H2_PORT")) GTEST_SKIP();
    auto runtime = rstd::async::RuntimeBuilder::multi_thread()
                       .worker_threads(usize(2))
                       .enable_all()
                       .build()
                       .unwrap();
    auto task    = [&]() -> coro<empty> {
        auto client = TestClient::make(runtime.handle(), h2_options()).unwrap();
        auto route  = h2_route();
        auto held   = (co_await h2_get(client, route, "/unread"_str)).unwrap();
        auto identity =
            String::make(*held.head().headers.get_unique_text("x-connection"_str).unwrap());
        auto body = held.take_body().unwrap();
        co_await rstd::async::sleep(Duration::from_millis(u64(30)));
        auto fast = (co_await h2_get(client, route, "/ok"_str)).unwrap();
        EXPECT_EQ(*fast.head().headers.get_unique_text("x-connection"_str).unwrap(),
                  identity.as_str());
        EXPECT_EQ((co_await h2_text(fast)).unwrap().as_str(), "hello"_str);
        body.cancel();
        auto next = (co_await h2_get(client, route, "/large"_str)).unwrap();
        EXPECT_EQ(*next.head().headers.get_unique_text("x-connection"_str).unwrap(),
                  identity.as_str());
        EXPECT_EQ((co_await h2_text(next)).unwrap().len(), usize(512 * 1024));
        EXPECT_TRUE((co_await client.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(task());
}
TEST(H2Peer, ResetGoawayAndDeadlines) {
    if (! ::getenv("LIFETCHTO_H2_PORT")) GTEST_SKIP();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        auto client = TestClient::make(runtime.handle(), h2_options()).unwrap();
        auto route  = h2_route();
        EXPECT_TRUE((co_await h2_get(client, route, "/reset"_str)).is_err());
        auto completed = (co_await h2_get(client, route, "/goaway"_str)).unwrap();
        auto identity =
            String::make(*completed.head().headers.get_unique_text("x-connection"_str).unwrap());
        EXPECT_EQ((co_await h2_text(completed)).unwrap().as_str(), "hello"_str);
        auto next = (co_await h2_get(client, route, "/ok"_str)).unwrap();
        EXPECT_NE(*next.head().headers.get_unique_text("x-connection"_str).unwrap(),
                  identity.as_str());
        EXPECT_TRUE((co_await h2_text(next)).is_ok());
        ExchangeOptions options;
        options.route    = Some(route.clone());
        options.deadline = Some(Instant::now() + Duration::from_millis(u64(80)));
        auto slow = (co_await client.send(TestRequest::make(h2_url("/slow"_str).as_str()).unwrap(),
                                          rstd::move(options)))
                        .unwrap();
        auto body = slow.take_body().unwrap();
        auto result = co_await body.next();
        EXPECT_TRUE(result.is_err());
        if (result.is_err()) EXPECT_TRUE(result.unwrap_err().is_Timeout());
        auto again = (co_await h2_get(client, route, "/ok"_str)).unwrap();
        EXPECT_TRUE((co_await h2_text(again)).is_ok());
        EXPECT_TRUE((co_await client.close()).is_ok());
        EXPECT_TRUE((co_await client.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(task());
}
TEST(H2Peer, AlpnFallbackAndForcedVersion) {
    if (! ::getenv("LIFETCHTO_H2_PORT")) GTEST_SKIP();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        for (auto version : array<VersionPolicy, 3> {
                 VersionPolicy::Automatic, VersionPolicy::Http1, VersionPolicy::Http2 }) {
            auto client = TestClient::make(runtime.handle(), h2_options(version)).unwrap();
            ExchangeOptions options;
            options.route = Some(h2_route("LIFETCHTO_H1_PORT"));
            auto response = co_await client.send(
                TestRequest::make(h2_url("/ok"_str, "LIFETCHTO_H1_PORT").as_str()).unwrap(),
                rstd::move(options));
            if (version == VersionPolicy::Http2)
                EXPECT_TRUE(response.is_err());
            else {
                EXPECT_TRUE(response.is_ok());
                if (response.is_ok()) {
                    EXPECT_EQ(response->head().version->major(), u8(1));
                    EXPECT_TRUE((co_await h2_text(*response)).is_ok());
                }
            }
            EXPECT_TRUE((co_await client.close()).is_ok());
        }
        auto client = TestClient::make(runtime.handle(), h2_options(VersionPolicy::Http1)).unwrap();
        auto route  = h2_route();
        auto response = (co_await h2_get(client, route, "/ok"_str)).unwrap();
        EXPECT_EQ(response.head().version->major(), u8(1));
        EXPECT_TRUE((co_await h2_text(response)).is_ok());
        EXPECT_TRUE((co_await client.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(task());
}

TEST(H2Peer, ProtocolErrorAndRepeatedStreamsReleaseTheConnection) {
    if (! ::getenv("LIFETCHTO_H2_PORT")) GTEST_SKIP();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        auto           client = TestClient::make(runtime.handle(), h2_options()).unwrap();
        auto           route  = h2_route();
        Option<String> identity;
        unsigned       iterations = 200;
        if (auto text = ::getenv("LIFETCHTO_H2_ITERATIONS"))
            iterations = static_cast<unsigned>(::atoi(text));
        EXPECT_TRUE(iterations > 0 && iterations <= 10000);
        if (iterations == 0 || iterations > 10000) co_return empty {};
        for (unsigned i = 0; i < iterations; ++i) {
            auto response   = (co_await h2_get(client, route, "/ok"_str)).unwrap();
            auto connection = *response.head().headers.get_unique_text("x-connection"_str).unwrap();
            if (identity.is_none())
                identity = Some(String::make(connection));
            else
                EXPECT_EQ(connection, identity->as_str());
            EXPECT_TRUE((co_await h2_text(response)).is_ok());
        }
        EXPECT_TRUE((co_await client.close()).is_ok());
        auto            bad = TestClient::make(runtime.handle(), h2_options()).unwrap();
        ExchangeOptions options;
        options.route = Some(h2_route("LIFETCHTO_H2_BAD_PORT"));
        auto started  = Instant::now();
        EXPECT_TRUE(
            (co_await bad.send(
                 TestRequest::make(h2_url("/ok"_str, "LIFETCHTO_H2_BAD_PORT").as_str()).unwrap(),
                 rstd::move(options)))
                .is_err());
        EXPECT_TRUE((co_await bad.close()).is_ok());
        EXPECT_TRUE(started.elapsed() < Duration::from_secs(u64(1)));
        co_return empty {};
    };
    runtime.block_on(task());
}
TEST(H2Peer, EarlyCompleteResponseSurvivesNoErrorReset) {
    if (! ::getenv("LIFETCHTO_H2_PORT")) GTEST_SKIP();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        auto client  = TestClient::make(runtime.handle(), h2_options()).unwrap();
        auto route   = h2_route();
        auto request = TestRequest::make(h2_url("/early-reset"_str).as_str(), "POST"_str).unwrap();
        request.body() = TestUploadBody::from_source(H2Upload {});
        ExchangeOptions options;
        options.route = Some(route.clone());
        auto response = (co_await client.send(rstd::move(request), rstd::move(options))).unwrap();
        EXPECT_EQ(response.head().status.value(), u16(413));
        EXPECT_TRUE((co_await h2_text(response)).unwrap().is_empty());
        auto next = (co_await h2_get(client, route, "/ok"_str)).unwrap();
        EXPECT_TRUE((co_await h2_text(next)).is_ok());
        EXPECT_TRUE((co_await client.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(task());
}
namespace
{
struct H2ClosingSource {
    using Error = lifetchto::Error;
    Arc<rstd::sync::Mutex<usize>> closes;
    bool                          failed {};
    auto next() -> coro<lifetchto::Result<Option<Bytes>>> { co_return Ok(None()); }
    auto close() -> coro<lifetchto::Result<empty>> {
        {
            auto count = closes->lock().unwrap();
            ++*count;
        }
        co_await rstd::async::sleep(Duration::from_millis(u64(80)));
        if (failed) co_return Err(lifetchto::Error::BodySource("h2 source close failed"_Str));
        co_return Ok(empty {});
    }
};
} // namespace
TEST(H2Peer, EngineCloseRetainsSourceAndNativeReleaseAcrossCanceledWait) {
    if (! ::getenv("LIFETCHTO_H2_PORT")) GTEST_SKIP();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        auto engine  = Engine::make(runtime.handle()).unwrap();
        auto options = h2_options();
        auto backend =
            http::NativeBackend::make(engine.resources(),
                                      rstd::move(static_cast<http::TransportOptions&>(options)))
                .unwrap();
        http::ClientConfig config;
        config.total_timeout = None();
        auto client          = http::Client::make(engine.handle(),
                                                  http::BackendBinding::Owned(backend.backend()),
                                                  rstd::move(config))
                                   .unwrap();
        auto closes          = Arc<rstd::sync::Mutex<usize>>::make(usize());
        auto url             = lihttpto::Url::parse_http(h2_url("/slow"_str).as_str()).unwrap();
        http::ExecutionRequest request {
            { rstd::move(url), lihttpto::Method::parse("POST"_str).unwrap(), {} },
            http::UploadPlan(lifetchto::UploadPlan::once(
                UploadBody::from_source(H2ClosingSource { closes.clone() }, Some(u64()))))
        };
        http::ExecuteOptions execute;
        execute.routing.route = Some(h2_route());
        auto transfer         = client.execute(rstd::move(request), rstd::move(execute)).unwrap();
        auto response         = (co_await transfer.output()).unwrap();
#if defined(__linux__)
        auto descriptors = h2_descriptors();
#endif
        auto waiting     = Some(engine.close().into_future());
        auto interrupted = co_await rstd::async::select(
            rstd::future::by_ref(*waiting), rstd::async::sleep(Duration::from_millis(u64(5))));
        EXPECT_TRUE(interrupted.is_right());
        waiting = None();
        EXPECT_TRUE((co_await engine.close()).is_ok());
        EXPECT_EQ(*closes->lock().unwrap(), usize(1));
#if defined(__linux__)
        EXPECT_TRUE(h2_descriptors() < descriptors);
#endif
        auto report = co_await transfer.control().wait();
        EXPECT_TRUE(report->cleanup.is_ok());
        EXPECT_TRUE((co_await response.body.next()).is_err());
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(task());
}
TEST(H2Peer, SourceReleaseFailureRemainsVisibleToClose) {
    if (! ::getenv("LIFETCHTO_H2_PORT")) GTEST_SKIP();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        auto client  = TestClient::make(runtime.handle(), h2_options()).unwrap();
        auto route   = h2_route();
        auto closes  = Arc<rstd::sync::Mutex<usize>>::make(usize());
        auto request = TestRequest::make(h2_url("/ok"_str).as_str(), "POST"_str).unwrap();
        request.body() =
            TestUploadBody::from_source(H2ClosingSource { closes.clone(), true }, Some(u64()));
        ExchangeOptions options;
        options.route = Some(route.clone());
        auto response = co_await client.send(rstd::move(request), rstd::move(options));
        if (response.is_ok()) (void)co_await h2_text(*response);
        EXPECT_TRUE((co_await client.close()).is_err());
        EXPECT_TRUE((co_await client.close()).is_err());
        EXPECT_EQ(*closes->lock().unwrap(), usize(1));
        co_return empty {};
    };
    runtime.block_on(task());
}
namespace
{
struct H2UploadGate {
    bool started {}, released {};
};
struct H2PausedUpload {
    using Error = lifetchto::Error;
    Arc<rstd::sync::Mutex<H2UploadGate>> gate;
    bool                                 sent {};
    auto                                 next() -> coro<lifetchto::Result<Option<Bytes>>> {
        if (sent) co_return Ok(None());
        {
            auto state     = gate->lock().unwrap();
            state->started = true;
        }
        for (;;) {
            bool released;
            {
                auto state = gate->lock().unwrap();
                released   = state->released;
            }
            if (released) break;
            co_await rstd::async::sleep(Duration::from_millis(u64(1)));
        }
        sent = true;
        co_return Ok(Some(Bytes::copy_from_slice("upload"_bytes)));
    }
};
} // namespace
TEST(H2Peer, PendingUploadSourcesDoNotBlockOtherStreams) {
    if (! ::getenv("LIFETCHTO_H2_PORT")) GTEST_SKIP();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        auto client    = TestClient::make(runtime.handle(), h2_options()).unwrap();
        auto route     = h2_route();
        auto gate      = Arc<rstd::sync::Mutex<H2UploadGate>>::make();
        auto request   = TestRequest::make(h2_url("/echo"_str).as_str(), "POST"_str).unwrap();
        request.body() = TestUploadBody::from_source(H2PausedUpload { gate.clone() });
        ExchangeOptions options;
        options.route = Some(route.clone());
        auto upload = runtime.handle().spawn(client.send(rstd::move(request), rstd::move(options)));
        for (;;) {
            bool started;
            {
                auto state = gate->lock().unwrap();
                started    = state->started;
            }
            if (started) break;
            co_await rstd::async::sleep(Duration::from_millis(u64(1)));
        }
        auto fast = (co_await h2_get(client, route, "/ok"_str)).unwrap();
        auto connection =
            String::make(*fast.head().headers.get_unique_text("x-connection"_str).unwrap());
        EXPECT_TRUE((co_await h2_text(fast)).is_ok());
        {
            auto state      = gate->lock().unwrap();
            state->released = true;
        }
        auto response = (co_await rstd::move(upload)).unwrap().unwrap();
        EXPECT_EQ(*response.head().headers.get_unique_text("x-connection"_str).unwrap(),
                  connection.as_str());
        EXPECT_EQ((co_await h2_text(response)).unwrap().as_str(), "upload"_str);
        EXPECT_TRUE((co_await client.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(task());
}

TEST(H2Peer, ZeroIdleBudgetDoesNotPollActiveRequests) {
    if (! ::getenv("LIFETCHTO_H2_PORT")) GTEST_SKIP();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        auto options         = h2_options();
        options.idle_timeout = Duration {};
        auto client          = TestClient::make(runtime.handle(), rstd::move(options)).unwrap();
        auto route           = h2_route();
        auto response        = (co_await h2_get(client, route, "/large"_str)).unwrap();
        EXPECT_EQ((co_await h2_text(response)).unwrap().len(), usize(512 * 1024));
        EXPECT_TRUE((co_await client.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(task());
}
