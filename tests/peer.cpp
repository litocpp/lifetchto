#include <rstd/test/gtest.hpp>
#include <stdlib.h>
#include <dirent.h>
#include "support.hpp"
using namespace rstd::prelude;
using namespace rstd::literals;
using namespace lifetchto;
using namespace test_http;
using rstd::async::coro;
using rstd::bytes::Bytes;
using rstd::sync::Arc;
using rstd::time::Duration;
using rstd::time::Instant;
namespace
{
auto traffic_request(ref<str> target, ref<str> method = "POST"_str) -> http::ExecutionRequest {
    return { { lihttpto::Url::parse_http(target).unwrap(),
               lihttpto::Method::parse(method).unwrap(),
               {} },
             http::UploadPlan::bytes(Bytes::copy_from_slice("abc"_bytes)) };
}
auto traffic_read(http::ResponseBody& body) -> coro<lifetchto::Result<usize>> {
    usize count;
    for (;;) {
        auto next = co_await body.next();
        if (next.is_err()) co_return Err(rstd::move(next).unwrap_err());
        if (next->is_none()) co_return Ok(count);
        count += (**next).len();
    }
}
struct TrafficSourceFailure {
    using Error = lifetchto::Error;
    bool sent {};
    auto next() -> coro<lifetchto::Result<Option<Bytes>>> {
        if (sent) co_return Err(Error::BodySource("upload stopped"_Str));
        sent = true;
        co_return Ok(Some(Bytes::copy_from_slice("abc"_bytes)));
    }
};
auto env_text(const char* name) -> ref<str> {
    return rstd::ffi::CStr::from_ptr(::getenv(name)).to_str().unwrap();
}
auto port(const char* name = "LIFETCHTO_PEER_PORT") -> u16 {
    return u16(::atoi(::getenv(name)));
}
auto url(ref<str> path) -> String {
    return rstd::format("http://127.0.0.1:{}{}", port(), path);
}
auto get(TestClient& client, ref<str> path) -> coro<lifetchto::Result<String>> {
    auto response = co_await client.send(TestRequest::make(url(path).as_str()).unwrap());
    if (response.is_err()) co_return Err(rstd::move(response).unwrap_err());
    auto body = response->take_body().unwrap();
    co_return co_await body.text(usize(1024 * 1024));
}
auto tls_options() -> TestClientOptions {
    lifetchto::http::TlsOptions trust;
    trust.use_system_roots = false;
    auto path              = rstd::path::PathBuf::from(env_text("LIFETCHTO_CA"));
    auto pem               = rstd::fs::read(path.as_path()).unwrap();
    trust.ca_pem.push(Bytes::copy_from_slice(pem.as_slice()));
    TestClientOptions options;
    options.tls = Some(TlsProfile::make(rstd::move(trust)).unwrap());
    return options;
}
auto fixed_route(const lihttpto::Url& url, u16 port) -> Arc<Route> {
    Vec<rstd::net::SocketAddr> addresses;
    addresses.push(rstd::net::SocketAddr::ipv4_loopback(port));
    return Arc<Route>::make(Route::tcp(url, rstd::move(addresses)).unwrap());
}
auto descriptors() -> int {
    auto dir   = ::opendir("/proc/self/fd");
    int  count = 0;
    while (::readdir(dir)) ++count;
    ::closedir(dir);
    return count;
}
struct Source {
    using Error = lifetchto::Error;
    usize                         remaining { 3 };
    Arc<rstd::sync::Mutex<usize>> pulls;
    bool                          delayed {};
    auto                          next() -> coro<lifetchto::Result<Option<Bytes>>> {
        {
            auto n = pulls->lock().unwrap();
            ++*n;
        }
        if (delayed) co_await rstd::async::sleep(Duration::from_millis(u64(500)));
        if (remaining == usize()) co_return Ok(None());
        --remaining;
        co_return Ok(Some(Bytes::copy_from_slice("abc"_str.as_bytes())));
    }
};
} // namespace
TEST(Peer, BodyTrafficCountsFixedChunkedAndRedirectBodiesWithoutFraming) {
    if (! ::getenv("LIFETCHTO_PEER_PORT")) GTEST_SKIP();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        auto               engine = Engine::make(runtime.handle()).unwrap();
        auto               native = http::NativeBackend::make(engine.resources()).unwrap();
        http::ClientConfig config;
        config.policy.redirect = Some(http::RedirectPolicy {});
        auto client            = http::Client::make(engine.handle(),
                                                    http::BackendBinding::Owned(native.backend()),
                                                    rstd::move(config))
                                     .unwrap();
        for (auto path : array<ref<str>, 3> { "/echo"_str, "/chunked"_str, "/adapter-307"_str }) {
            http::ExecutionObserver observer;
            http::ExecuteOptions    options;
            options.report = Some(observer.clone());
            auto request   = traffic_request(url(path).as_str());
            if (path == "/chunked"_str) {
                lihttpto::Headers trailers;
                trailers.add("X-End"_str, "abc"_str).unwrap();
                request.body = http::UploadPlan::bytes(Bytes::copy_from_slice("abc"_bytes),
                                                       Some(rstd::move(trailers)));
            }
            auto transfer = client.execute(rstd::move(request), rstd::move(options)).unwrap();
            auto response = (co_await transfer.output()).unwrap();
            EXPECT_EQ((co_await traffic_read(response.body)).unwrap(),
                      path == "/chunked"_str ? usize(5) : usize(3));
            auto report = (co_await observer.wait()).unwrap();
            EXPECT_TRUE(report.transfer->logical.result.is_ok());
            auto progress = observer.snapshot().body_progress;
            EXPECT_EQ(progress.body_sent_bytes, Some(path == "/adapter-307"_str ? u64(6) : u64(3)));
            EXPECT_EQ(progress.body_received_bytes,
                      Some(path == "/adapter-307"_str ? u64(8)
                           : path == "/chunked"_str   ? u64(5)
                                                      : u64(3)));
            EXPECT_EQ(report.facts.body_progress.body_sent_bytes, progress.body_sent_bytes);
            EXPECT_EQ(report.facts.body_progress.body_received_bytes, progress.body_received_bytes);
        }
        EXPECT_TRUE((co_await client.close()).is_ok());
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(run());
}
TEST(Peer, BodyTrafficRetainsReceivedBytesAfterErrorAndUnreadCancellation) {
    if (! ::getenv("LIFETCHTO_PEER_PORT")) GTEST_SKIP();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        auto                   engine = Engine::make(runtime.handle()).unwrap();
        http::TransportOptions transport;
        transport.receive_window = usize(8);
        auto native = http::NativeBackend::make(engine.resources(), rstd::move(transport)).unwrap();
        auto client =
            http::Client::make(engine.handle(), http::BackendBinding::Owned(native.backend()))
                .unwrap();
        for (auto path : array<ref<str>, 2> { "/truncated"_str, "/large"_str }) {
            http::ExecutionObserver observer;
            http::ExecuteOptions    options;
            options.report = Some(observer.clone());
            auto transfer =
                client.execute(traffic_request(url(path).as_str()), rstd::move(options)).unwrap();
            auto response = (co_await transfer.output()).unwrap();
            if (path == "/truncated"_str) {
                EXPECT_TRUE((co_await traffic_read(response.body)).is_err());
            } else {
                while (observer.snapshot().body_progress.body_received_bytes.unwrap_or(u64()) <
                       u64(8))
                    co_await rstd::async::yield_now();
                EXPECT_EQ(observer.snapshot().body_progress.body_received_bytes, Some(u64(8)));
                response.body.cancel();
            }
            auto report = (co_await observer.wait()).unwrap();
            EXPECT_TRUE(report.transfer->logical.result.is_err());
            EXPECT_EQ(report.facts.body_progress.body_sent_bytes, Some(u64(3)));
            EXPECT_EQ(report.facts.body_progress.body_received_bytes,
                      Some(path == "/truncated"_str ? u64(2) : u64(8)));
            EXPECT_EQ(observer.snapshot().body_progress.body_received_bytes,
                      report.facts.body_progress.body_received_bytes);
        }
        EXPECT_TRUE((co_await client.close()).is_ok());
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(run());
}
TEST(Peer, BodyTrafficObserverSurvivesIndependentNativeAttemptAndClose) {
    if (! ::getenv("LIFETCHTO_PEER_PORT")) GTEST_SKIP();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        http::BodyTrafficObserver observer;
        EXPECT_TRUE(observer.snapshot().body_sent_bytes.is_none());
        auto engine   = Engine::make(runtime.handle()).unwrap();
        auto native   = http::NativeBackend::make(engine.resources()).unwrap();
        auto request  = traffic_request(url("/echo"_str).as_str());
        auto prepared = native.prepare(rstd::move(request.parts), request.body.metadata()).unwrap();
        auto body =
            http::UploadBody(lifetchto::UploadBody::bytes(Bytes::copy_from_slice("abc"_bytes)));
        auto operation =
            native.operation({ rstd::move(prepared), rstd::move(body), Some(observer.clone()) })
                .unwrap();
        EXPECT_EQ(observer.snapshot().body_sent_bytes, Some(u64()));
        auto transfer = engine.submit(rstd::move(operation)).unwrap();
        auto response = (co_await transfer.output()).unwrap();
        EXPECT_EQ((co_await traffic_read(response.body)).unwrap(), usize(3));
        EXPECT_TRUE((co_await transfer.control().wait())->logical.result.is_ok());
        EXPECT_TRUE((co_await native.close()).is_ok());
        EXPECT_TRUE((co_await engine.close()).is_ok());
        EXPECT_EQ(observer.snapshot().body_sent_bytes, Some(u64(3)));
        EXPECT_EQ(observer.snapshot().body_received_bytes, Some(u64(3)));
        co_return empty {};
    };
    runtime.block_on(run());
}
TEST(Peer, NativeAttemptKeepsFailureForItsConsumer) {
    if (! ::getenv("LIFETCHTO_PEER_PORT")) GTEST_SKIP();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        auto engine = Engine::make(runtime.handle()).unwrap();
        auto native = http::NativeBackend::make(engine.resources()).unwrap();
        for (bool body_failure : array<bool, 2> { false, true }) {
            auto request =
                traffic_request(url(body_failure ? "/truncated"_str : "/invalid"_str).as_str());
            auto   prepared  = native.prepare(rstd::move(request.parts), {}).unwrap();
            auto   operation = native.operation({ rstd::move(prepared), {} }).unwrap();
            auto   transfer  = engine.submit(rstd::move(operation)).unwrap();
            auto   response  = co_await transfer.output();
            String diagnostic;
            if (body_failure) {
                EXPECT_TRUE(response.is_ok());
                auto read = co_await traffic_read(response->body);
                EXPECT_TRUE(read.as_ref().unwrap_err().is_Protocol());
                diagnostic  = rstd::format("{}", read.as_ref().unwrap_err());
                auto report = co_await transfer.control().wait();
                EXPECT_EQ(report->logical.result.as_ref().unwrap_err().message, diagnostic);
                EXPECT_TRUE(transfer.take_error()->is_Closed());
                EXPECT_TRUE((co_await response->body.next()).unwrap_err().is_Closed());
            } else {
                EXPECT_TRUE(response.as_ref().unwrap_err().is_Protocol());
                diagnostic  = rstd::format("{}", response.as_ref().unwrap_err());
                auto report = co_await transfer.control().wait();
                EXPECT_EQ(report->logical.result.as_ref().unwrap_err().message, diagnostic);
            }
            EXPECT_TRUE(transfer.take_error().is_none());
        }
        EXPECT_TRUE((co_await native.close()).is_ok());
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(run());
}

TEST(Peer, BodyTrafficRetainsConfirmedUploadWhenLaterSourceFails) {
    if (! ::getenv("LIFETCHTO_PEER_PORT")) GTEST_SKIP();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        auto engine = Engine::make(runtime.handle()).unwrap();
        auto native = http::NativeBackend::make(engine.resources()).unwrap();
        auto client =
            http::Client::make(engine.handle(), http::BackendBinding::Owned(native.backend()))
                .unwrap();
        http::ExecutionObserver observer;
        http::ExecuteOptions    options;
        options.report = Some(observer.clone());
        auto request   = traffic_request(url("/echo"_str).as_str());
        request.body =
            http::UploadPlan::once(lifetchto::UploadBody::from_source(TrafficSourceFailure {}));
        auto transfer = client.execute(rstd::move(request), rstd::move(options)).unwrap();
        EXPECT_TRUE((co_await transfer.output()).is_err());
        auto report = (co_await observer.wait()).unwrap();
        EXPECT_TRUE(report.transfer->logical.result.is_err());
        EXPECT_EQ(report.facts.body_progress.body_sent_bytes, Some(u64(3)));
        EXPECT_EQ(report.facts.body_progress.body_received_bytes, Some(u64()));
        EXPECT_TRUE((co_await client.close()).is_ok());
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(run());
}
TEST(Peer, FramingAndStreaming) {
    if (! ::getenv("LIFETCHTO_PEER_PORT")) GTEST_SKIP();
    auto runtime = rstd::async::RuntimeBuilder::multi_thread()
                       .worker_threads(usize(2))
                       .enable_all()
                       .build()
                       .unwrap();
    auto task    = [&]() -> coro<empty> {
        TestClientOptions options;
        options.receive_window = usize(1024);
        auto client            = TestClient::make(runtime.handle(), rstd::move(options)).unwrap();
        auto local = (co_await client.send(
                          TestRequest::make(rstd::format("http://localhost:{}/ok", port()).as_str())
                              .unwrap()))
                         .unwrap();
        auto local_body = local.take_body().unwrap();
        EXPECT_EQ((co_await local_body.text(usize(100))).unwrap().as_str(), "hello"_str);
        for (auto path :
             array<ref<str>, 4> { "/ok"_str, "/informational"_str, "/chunked"_str, "/eof"_str }) {
            auto result = co_await get(client, path);
            EXPECT_TRUE(result.is_ok());
            if (result.is_ok())
                EXPECT_EQ(result->as_str(),
                          path == "/chunked"_str ? "abcde"_str
                          : path == "/eof"_str   ? "eof-body"_str
                                                 : "hello"_str);
        }
        auto response = co_await client.send(TestRequest::make(url("/ok"_str).as_str()).unwrap());
        EXPECT_TRUE(response.is_ok());
        if (response.is_ok()) {
            EXPECT_EQ(response->head().headers.get_all("x-dup"_str).len(), usize(2));
            EXPECT_EQ((*response->head().headers.get("x-bin"_str))->as_slice()[usize()], u8(255));
            auto body = response->take_body().unwrap();
            EXPECT_TRUE(response->take_body().is_err());
            EXPECT_TRUE((co_await body.bytes(usize(100))).is_ok());
        }
        auto large = co_await client.send(TestRequest::make(url("/large"_str).as_str()).unwrap());
        EXPECT_TRUE(large.is_ok());
        if (large.is_ok()) {
            auto  body = large->take_body().unwrap();
            usize count {};
            for (;;) {
                auto part = co_await body.next();
                EXPECT_TRUE(part.is_ok());
                if (part.is_err() || part->is_none()) break;
                EXPECT_TRUE((**part).len() <= usize(1024));
                count += (**part).len();
            }
            EXPECT_EQ(count, usize(512 * 1024));
        }
        EXPECT_TRUE((co_await client.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(task());
}
TEST(Peer, TrailersAndNoBodyStatuses) {
    if (! ::getenv("LIFETCHTO_PEER_PORT")) GTEST_SKIP();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        auto client = TestClient::make(runtime.handle()).unwrap();
        auto response =
            (co_await client.send(TestRequest::make(url("/chunked"_str).as_str()).unwrap()))
                .unwrap();
        auto body = response.take_body().unwrap();
        EXPECT_TRUE(body.trailers().unwrap().is_none());
        EXPECT_EQ((co_await body.text(usize(100))).unwrap().as_str(), "abcde"_str);
        EXPECT_EQ(*body.trailers().unwrap()->get_unique_text("x-end"_str).unwrap(), "yes"_str);
        for (auto path : array<ref<str>, 3> { "/empty"_str, "/204"_str, "/304"_str })
            EXPECT_TRUE((co_await get(client, path)).unwrap().is_empty());
        auto head =
            (co_await client.send(TestRequest::make(url("/ok"_str).as_str(), "HEAD"_str).unwrap()))
                .unwrap();
        auto head_body = head.take_body().unwrap();
        EXPECT_TRUE((co_await head_body.bytes(usize())).unwrap().is_empty());
        EXPECT_TRUE((co_await client.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(task());
}
TEST(Peer, UploadLengthChunkingAndEarlyResponse) {
    if (! ::getenv("LIFETCHTO_PEER_PORT")) GTEST_SKIP();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        auto client = TestClient::make(runtime.handle()).unwrap();
        for (bool known : array<bool, 2> { false, true }) {
            auto pulls     = Arc<rstd::sync::Mutex<usize>>::make(usize());
            auto request   = TestRequest::make(url("/upload"_str).as_str(), "POST"_str).unwrap();
            request.body() = TestUploadBody::from_source(Source { usize(3), pulls.clone(), false },
                                                         known ? Some(u64(6)) : None<u64>());
            if (! known) {
                lihttpto::Headers trailers;
                trailers.add("X-End"_str, "yes"_str).unwrap();
                request.body().set_trailers(rstd::move(trailers));
            }
            request.parts().headers.add("Expect"_str, "100-continue"_str).unwrap();
            auto response = (co_await client.send(rstd::move(request))).unwrap();
            auto body     = response.take_body().unwrap();
            EXPECT_EQ((co_await body.text(usize(100))).unwrap().as_str(),
                      known ? "abcabc"_str : "abcabcabc"_str);
            EXPECT_EQ(*pulls->lock().unwrap(), known ? usize(2) : usize(4));
        }
        auto pulls     = Arc<rstd::sync::Mutex<usize>>::make(usize());
        auto request   = TestRequest::make(url("/early"_str).as_str(), "POST"_str).unwrap();
        request.body() = TestUploadBody::from_source(Source { usize(3), pulls.clone(), true });
        auto started   = Instant::now();
        auto response  = (co_await client.send(rstd::move(request))).unwrap();
        EXPECT_EQ(response.head().status.value(), u16(413));
        EXPECT_TRUE(started.elapsed() < Duration::from_millis(u64(400)));
        EXPECT_TRUE(*pulls->lock().unwrap() <= usize(1));
        auto body = response.take_body().unwrap();
        EXPECT_TRUE((co_await body.bytes(usize())).is_ok());
        EXPECT_TRUE((co_await client.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(task());
}
TEST(Peer, ProtocolFailuresAndCollectionLimit) {
    if (! ::getenv("LIFETCHTO_PEER_PORT")) GTEST_SKIP();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        auto client = TestClient::make(runtime.handle()).unwrap();
        auto invalid =
            co_await client.send(TestRequest::make(url("/invalid"_str).as_str()).unwrap());
        EXPECT_TRUE(invalid.is_err());
        auto truncated = co_await get(client, "/truncated"_str);
        EXPECT_TRUE(truncated.is_err());
        auto response =
            (co_await client.send(TestRequest::make(url("/large"_str).as_str()).unwrap())).unwrap();
        auto body = response.take_body().unwrap();
        auto over = co_await body.bytes(usize(100));
        EXPECT_TRUE(over.is_err());
        if (over.is_err()) EXPECT_TRUE(over.unwrap_err().is_TooLarge());
        EXPECT_TRUE((co_await client.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(task());
}
TEST(Peer, HeadDeliveryAndAbsoluteBodyDeadline) {
    if (! ::getenv("LIFETCHTO_PEER_PORT")) GTEST_SKIP();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        TestClientOptions options;
        options.total_timeout = Some(Duration::from_millis(u64(100)));
        auto client           = TestClient::make(runtime.handle(), rstd::move(options)).unwrap();
        auto started          = Instant::now();
        auto response =
            (co_await client.send(TestRequest::make(url("/slow-body"_str).as_str()).unwrap()))
                .unwrap();
        EXPECT_TRUE(started.elapsed() < Duration::from_millis(u64(80)));
        auto body   = response.take_body().unwrap();
        auto result = co_await body.bytes(usize(100));
        EXPECT_TRUE(result.is_err());
        if (result.is_err()) EXPECT_TRUE(result.unwrap_err().is_Timeout());
        auto waiting =
            co_await client.send(TestRequest::make(url("/stall-head"_str).as_str()).unwrap());
        EXPECT_TRUE(waiting.is_err());
        if (waiting.is_err()) EXPECT_TRUE(waiting.unwrap_err().is_Timeout());
        EXPECT_TRUE((co_await client.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(task());
}
TEST(Peer, CloseBarrierIdleInputAndNoReplay) {
    if (! ::getenv("LIFETCHTO_PEER_PORT")) GTEST_SKIP();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        auto client = TestClient::make(runtime.handle()).unwrap();
        EXPECT_TRUE((co_await get(client, "/idle-extra"_str)).is_ok());
        co_await rstd::async::sleep(Duration::from_millis(u64(80)));
        EXPECT_TRUE((co_await get(client, "/ok"_str)).is_ok());
        EXPECT_TRUE((co_await get(client, "/idle-eof"_str)).is_ok());
        co_await rstd::async::sleep(Duration::from_millis(u64(20)));
        EXPECT_TRUE((co_await get(client, "/ok"_str)).is_ok());
        auto response =
            (co_await client.send(TestRequest::make(url("/slow-body"_str).as_str()).unwrap()))
                .unwrap();
        auto count = descriptors();
        EXPECT_TRUE((co_await client.close()).is_ok());
        EXPECT_TRUE(descriptors() < count);
        auto body = response.take_body().unwrap();
        EXPECT_TRUE((co_await body.next()).is_err());
        EXPECT_TRUE((co_await client.close()).is_ok());
        EXPECT_TRUE((co_await client.send(TestRequest::make(url("/ok"_str).as_str()).unwrap()))
                        .unwrap_err()
                        .is_Closed());
        co_return empty {};
    };
    runtime.block_on(task());
}
TEST(Peer, TlsIdentityRouteAndUncleanEof) {
    if (! ::getenv("LIFETCHTO_PEER_PORT")) GTEST_SKIP();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        auto client = TestClient::make(runtime.handle(), tls_options()).unwrap();
        for (auto host : array<ref<str>, 3> { "origin.test"_str, "127.0.0.1"_str, "[::1]"_str }) {
            auto request =
                TestRequest::make(
                    rstd::format("https://{}:{}/ok", host, port("LIFETCHTO_TLS_PORT")).as_str())
                    .unwrap();
            ExchangeOptions options;
            options.route = Some(fixed_route(request.parts().url, port("LIFETCHTO_TLS_PORT")));
            auto response = co_await client.exchange(rstd::move(request), rstd::move(options));
            EXPECT_TRUE(response.is_ok());
            if (response.is_ok()) {
                auto body = response->take_body().unwrap();
                EXPECT_EQ((co_await body.text(usize(100))).unwrap().as_str(), "hello"_str);
            }
        }
        auto idle           = TestRequest::make(rstd::format("https://127.0.0.1:{}/idle-unclean",
                                                             port("LIFETCHTO_TLS_PORT"))
                                                    .as_str())
                                  .unwrap();
        auto completed      = (co_await client.send(rstd::move(idle))).unwrap();
        auto completed_body = completed.take_body().unwrap();
        EXPECT_EQ((co_await completed_body.text(usize(100))).unwrap().as_str(), "hello"_str);
        co_await rstd::async::sleep(Duration::from_millis(u64(20)));
        auto wrong =
            TestRequest::make(
                rstd::format("https://wrong.test:{}/ok", port("LIFETCHTO_TLS_PORT")).as_str())
                .unwrap();
        ExchangeOptions wrong_options;
        wrong_options.route = Some(fixed_route(wrong.parts().url, port("LIFETCHTO_TLS_PORT")));
        auto failed = co_await client.exchange(rstd::move(wrong), rstd::move(wrong_options));
        EXPECT_TRUE(failed.is_err());
        if (failed.is_err()) EXPECT_TRUE(failed.unwrap_err().is_Extension());
        auto unclean =
            TestRequest::make(
                rstd::format("https://127.0.0.1:{}/unclean", port("LIFETCHTO_TLS_PORT")).as_str())
                .unwrap();
        auto response = (co_await client.send(rstd::move(unclean))).unwrap();
        auto body     = response.take_body().unwrap();
        EXPECT_TRUE((co_await body.bytes(usize(100))).is_err());
        EXPECT_TRUE((co_await client.close()).is_ok());
        auto untrusted = TestClient::make(runtime.handle()).unwrap();
        auto request =
            TestRequest::make(
                rstd::format("https://127.0.0.1:{}/ok", port("LIFETCHTO_TLS_PORT")).as_str())
                .unwrap();
        EXPECT_TRUE((co_await untrusted.send(rstd::move(request))).is_err());
        EXPECT_TRUE((co_await untrusted.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(task());
}
TEST(Peer, PublicExchangeComposesRedirect) {
    if (! ::getenv("LIFETCHTO_PEER_PORT")) GTEST_SKIP();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        auto            client  = TestClient::make(runtime.handle()).unwrap();
        auto            first   = TestRequest::make(url("/redirect"_str).as_str()).unwrap();
        auto            current = first.parts().clone();
        ExchangeOptions options;
        options.deadline = Some(Instant::now() + Duration::from_secs(u64(2)));
        auto response = (co_await client.exchange(rstd::move(first), rstd::move(options))).unwrap();
        EXPECT_EQ(response.head().status.value(), u16(302));
        auto target =
            current.url.resolve(*response.head().headers.get_unique_text("location"_str).unwrap())
                .unwrap();
        auto old = response.take_body().unwrap();
        EXPECT_TRUE((co_await old.bytes(usize())).is_ok());
        current.url = rstd::move(target);
        ExchangeOptions remaining;
        remaining.deadline = response.deadline();
        auto next =
            (co_await client.exchange(TestRequest(rstd::move(current)), rstd::move(remaining)))
                .unwrap();
        auto body = next.take_body().unwrap();
        EXPECT_EQ((co_await body.text(usize(100))).unwrap().as_str(), "hello"_str);
        EXPECT_TRUE((co_await client.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(task());
}

TEST(Peer, PoolCapacityRouteIsolationAndCanceledWaiter) {
    if (! ::getenv("LIFETCHTO_PEER_PORT")) GTEST_SKIP();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        TestClientOptions limits;
        limits.max_connections            = usize(1);
        limits.max_connections_per_origin = usize(1);
        auto client = TestClient::make(runtime.handle(), rstd::move(limits)).unwrap();
        auto held =
            (co_await client.send(TestRequest::make(url("/slow-body"_str).as_str()).unwrap()))
                .unwrap();
        ExchangeOptions wait;
        wait.deadline = Some(Instant::now() + Duration::from_millis(u64(20)));
        auto timeout = co_await client.exchange(TestRequest::make(url("/ok"_str).as_str()).unwrap(),
                                                rstd::move(wait));
        EXPECT_TRUE(timeout.is_err());
        if (timeout.is_err()) EXPECT_TRUE(timeout.unwrap_err().is_Timeout());
        {
            auto body = held.take_body().unwrap();
            body.cancel();
        }
        EXPECT_EQ((co_await get(client, "/ok"_str)).unwrap().as_str(), "hello"_str);
        for (int i = 0; i < 3; ++i) {
            auto req =
                TestRequest::make(rstd::format("http://route.test:{}/route-{}", port(), i).as_str())
                    .unwrap();
            ExchangeOptions options;
            options.route = Some(fixed_route(req.parts().url, port()));
            auto response =
                (co_await client.exchange(rstd::move(req), rstd::move(options))).unwrap();
            auto body = response.take_body().unwrap();
            EXPECT_EQ((co_await body.text(usize(100))).unwrap().as_str(), "hello"_str);
        }
        EXPECT_TRUE((co_await client.close()).is_ok());
        auto no_replay = TestClient::make(runtime.handle()).unwrap();
        auto post      = TestRequest::make(url("/close-request"_str).as_str(), "POST"_str).unwrap();
        post.body()    = TestUploadBody::bytes(Bytes::copy_from_slice("once"_str.as_bytes()));
        EXPECT_TRUE((co_await no_replay.send(rstd::move(post))).is_err());
        EXPECT_TRUE((co_await no_replay.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(task());
}
TEST(Peer, UnixRouteRetainsLogicalHost) {
    if (! ::getenv("LIFETCHTO_UNIX_PATH")) GTEST_SKIP();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        auto client = TestClient::make(runtime.handle()).unwrap();
        auto req    = TestRequest::make("http://unix.invalid/ok"_str).unwrap();
        auto route  = Arc<Route>::make(
            Route::unix_socket(req.parts().url,
                               Bytes::copy_from_slice(env_text("LIFETCHTO_UNIX_PATH").as_bytes()))
                .unwrap());
        ExchangeOptions options;
        options.route = Some(route.clone());
        auto response = (co_await client.exchange(rstd::move(req), rstd::move(options))).unwrap();
        auto body     = response.take_body().unwrap();
        EXPECT_EQ((co_await body.text(usize(100))).unwrap().as_str(), "hello"_str);
        EXPECT_TRUE((co_await client.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(task());
}

TEST(Peer, TlsNoAlpnExpiryAndHandshakeDeadline) {
    if (! ::getenv("LIFETCHTO_NO_ALPN_PORT")) GTEST_SKIP();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        auto client = TestClient::make(runtime.handle(), tls_options()).unwrap();
        auto req =
            TestRequest::make(
                rstd::format("https://localhost:{}/ok", port("LIFETCHTO_NO_ALPN_PORT")).as_str())
                .unwrap();
        auto response = (co_await client.send(rstd::move(req))).unwrap();
        auto body     = response.take_body().unwrap();
        EXPECT_EQ((co_await body.text(usize(100))).unwrap().as_str(), "hello"_str);
        auto expired =
            TestRequest::make(
                rstd::format("https://127.0.0.1:{}/ok", port("LIFETCHTO_EXPIRED_PORT")).as_str())
                .unwrap();
        auto rejected = co_await client.send(rstd::move(expired));
        EXPECT_TRUE(rejected.is_err());
        if (rejected.is_err()) EXPECT_TRUE(rejected.unwrap_err().is_Extension());
        EXPECT_TRUE((co_await client.close()).is_ok());
        auto options            = tls_options();
        options.connect_timeout = Duration::from_millis(u64(80));
        auto stalled            = TestClient::make(runtime.handle(), rstd::move(options)).unwrap();
        auto target =
            TestRequest::make(
                rstd::format("https://127.0.0.1:{}/ok", port("LIFETCHTO_STALL_PORT")).as_str())
                .unwrap();
        auto started = Instant::now();
        auto query   = runtime.handle().spawn(stalled.send(rstd::move(target)));
        co_await rstd::async::sleep(Duration::from_millis(u64(10)));
        EXPECT_TRUE((co_await stalled.close()).is_ok());
        EXPECT_TRUE(started.elapsed() < Duration::from_millis(u64(500)));
        EXPECT_TRUE((co_await rstd::move(query)).unwrap().is_err());
        co_return empty {};
    };
    runtime.block_on(task());
}

namespace
{
struct FailedSourceClose {
    using Error = lifetchto::Error;
    Arc<rstd::sync::Mutex<usize>> closes;
    auto next() -> coro<lifetchto::Result<Option<Bytes>>> { co_return Ok(None()); }
    auto close() -> coro<lifetchto::Result<empty>> {
        {
            auto f = closes->lock().unwrap();
            ++*f;
        }
        co_return Err(lifetchto::Error::BodySource(String::make("source release failed"_str)));
    }
};
} // namespace
TEST(Peer, SourceReleaseFailureRemainsVisibleToClose) {
    if (! ::getenv("LIFETCHTO_PEER_PORT")) GTEST_SKIP();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        auto client = TestClient::make(runtime.handle()).unwrap();
        auto closes = Arc<rstd::sync::Mutex<usize>>::make(usize());
        auto req    = TestRequest::make(url("/empty"_str).as_str(), "POST"_str).unwrap();
        req.body() = TestUploadBody::from_source(FailedSourceClose { closes.clone() }, Some(u64()));
        auto result = co_await client.send(rstd::move(req));
        if (result.is_ok()) {
            auto body = result->take_body().unwrap();
            EXPECT_TRUE((co_await body.bytes(usize())).is_err());
        } else
            EXPECT_TRUE(result.unwrap_err().is_Closed());
        auto closed = co_await client.close();
        EXPECT_TRUE(closed.is_err());
        if (closed.is_err()) EXPECT_TRUE(closed.unwrap_err().is_BodySource());
        EXPECT_TRUE((co_await client.wait_closed()).is_err());
        EXPECT_EQ(*closes->lock().unwrap(), usize(1));
        co_return empty {};
    };
    runtime.block_on(task());
}
