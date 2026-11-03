#include <rstd/test/gtest.hpp>
#include <stdlib.h>
import rstd;
import lifetchto;
using namespace rstd::prelude;
using namespace rstd::literals;
using namespace lifetchto;
namespace tunnel_test
{
using lifetchto::Result;
struct GateFields {
    bool armed {}, blocked {}, released {}, stopped {};
};
struct Gate {
    rstd::sync::Mutex<GateFields> fields { GateFields {} };
    Event                         changed;
    void                          release() const {
        fields.lock().unwrap()->released = true;
        changed.notify();
    }
};
struct WireState {
    rstd::async::io::Stream native;
    Arc<Gate>               gate;
    WireState(rstd::async::io::Stream native, Arc<Gate> gate)
        : native(rstd::move(native)), gate(rstd::move(gate)) {}
};
auto gated_send(Arc<WireState> state, Bytes bytes) -> coro<rstd::io::Result<usize>> {
    auto size   = bytes.len();
    auto result = co_await state->native.send_all(rstd::move(bytes));
    if (result.is_err()) co_return rstd::move(result);
    bool hold;
    {
        auto f = state->gate->fields.lock().unwrap();
        // Let small HTTP/2 control records pass while holding the upload completion.
        hold = f->armed && ! f->stopped && size >= usize(1024);
        if (hold) {
            f->armed   = false;
            f->blocked = true;
        }
    }
    if (hold) {
        state->gate->changed.notify();
        for (;;) {
            auto event = state->gate->changed.listen();
            {
                auto f = state->gate->fields.lock().unwrap();
                if (f->released || f->stopped) break;
            }
            co_await rstd::move(event);
        }
    }
    co_return rstd::move(result);
}
struct Wire {
    Arc<WireState> state;
    auto           recv_into(rstd::bytes::BytesMut buffer) const {
        return state->native.recv_into(rstd::move(buffer));
    }
    auto send(Bytes bytes) const { return gated_send(state.clone(), rstd::move(bytes)); }
    auto flush() const { return state->native.flush(); }
    auto shutdown_write() const { return state->native.shutdown_write(); }
    void request_stop() const {
        state->gate->fields.lock().unwrap()->stopped = true;
        state->gate->changed.notify();
        state->native.request_stop();
    }
    auto wait_closed() const { return state->native.wait_closed(); }
    auto take_close_result() const { return state->native.take_close_result(); }
};
struct Dial {
    Arc<rstd::sync::Mutex<Option<rstd::net::TcpConnect>>> connecting;
    Arc<Gate>                                             gate;
    auto                                                  finish() const -> coro<Result<empty>> {
        auto operation = [&] {
            auto f = connecting->lock().unwrap();
            return (*f)->finish();
        }();
        auto result = co_await rstd::move(operation);
        if (result.is_err()) co_return Err(Error::Connect(result.unwrap_err()));
        co_return Ok(empty {});
    }
    auto into_stream() const -> Result<rstd::async::io::Stream>;
    auto close() const -> coro<Result<empty>> {
        auto pending = connecting->lock().unwrap()->take();
        if (pending.is_some()) (void)co_await rstd::move(*pending).close();
        co_return Ok(empty {});
    }
};
struct Connector {
    Arc<Gate> gate;
    auto      prepare(rstd::net::SocketAddr address) const -> Result<Arc<rstd::dyn<http::TcpDial>>>;
};
} // namespace tunnel_test
namespace rstd
{
template<>
struct Impl<async::io::CompletionStream, tunnel_test::Wire> : ImplBase<tunnel_test::Wire> {
    auto recv_into(bytes::BytesMut buffer) const {
        return this->self().recv_into(rstd::move(buffer));
    }
    auto send(bytes::Bytes bytes) const { return this->self().send(rstd::move(bytes)); }
    auto flush() const { return this->self().flush(); }
    auto shutdown_write() const { return this->self().shutdown_write(); }
    void request_stop() const { this->self().request_stop(); }
    auto wait_closed() const { return this->self().wait_closed(); }
    auto take_close_result() const { return this->self().take_close_result(); }
};
template<>
struct Impl<http::TcpDial, tunnel_test::Dial> : ImplBase<tunnel_test::Dial> {
    auto finish() const { return this->self().finish(); }
    auto into_stream() const { return this->self().into_stream(); }
    auto close() const { return this->self().close(); }
};
template<>
struct Impl<http::TcpConnector, tunnel_test::Connector> : ImplBase<tunnel_test::Connector> {
    auto prepare(net::SocketAddr address) const { return this->self().prepare(address); }
};
} // namespace rstd
namespace tunnel_test
{
auto Dial::into_stream() const -> Result<rstd::async::io::Stream> {
    auto pending   = connecting->lock().unwrap()->take().unwrap();
    auto connected = rstd::move(pending).into_stream();
    if (connected.is_err()) return Err(Error::Connect(connected.unwrap_err()));
    auto native = rstd::net::as_stream(rstd::move(*connected));
    if (native.is_err()) return Err(Error::Connect(native.unwrap_err()));
    return Ok(rstd::async::io::Stream(Arc<rstd::dyn<rstd::async::io::CompletionStream>>::make(
        Wire { Arc<WireState>::make(rstd::move(*native), gate.clone()) })));
}
auto Connector::prepare(rstd::net::SocketAddr address) const
    -> Result<Arc<rstd::dyn<http::TcpDial>>> {
    auto pending = rstd::net::TcpStream::prepare_connect(address);
    if (pending.is_err()) return Err(Error::Connect(pending.unwrap_err()));
    return Ok(Arc<rstd::dyn<http::TcpDial>>::make(Dial {
        Arc<rstd::sync::Mutex<Option<rstd::net::TcpConnect>>>::make(Some(rstd::move(*pending))),
        gate.clone() }));
}
} // namespace tunnel_test
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
auto tunnel_client(Engine&                                    engine,
                   Duration                                   timeout = Duration::from_secs(u64(2)),
                   const char*                                port_name = "LIFETCHTO_H2_PORT",
                   Option<Arc<rstd::dyn<http::TcpConnector>>> connector = {},
                   Option<usize> stream_limit = {}) -> http::TunnelClient {
    auto port = u16(::atoi(::getenv(port_name)));
    auto url =
        lihttpto::Url::parse_http(rstd::format("https://origin.test:{}/", port).as_str()).unwrap();
    Vec<rstd::net::SocketAddr> addresses;
    addresses.push(rstd::net::SocketAddr::ipv4_loopback(port));
    http::RouteChoice route { Some(Arc<http::Route>::make(
        http::Route::tcp(url, rstd::move(addresses), u64(), rstd::move(connector)).unwrap())) };
    auto              options = tunnel_options();
    options.connect_timeout   = timeout;
    if (stream_limit.is_some()) options.max_http2_streams = *stream_limit;
    return http::TunnelClient::make(
               engine.resources(), rstd::move(url), rstd::move(route), rstd::move(options))
        .unwrap();
}
template<class T>
auto tunnel_cause(rstd::error::ErrorRef error) -> Option<ref<T>> {
    for (unsigned depth = 0; depth < 12; ++depth) {
        auto typed = rstd::error::downcast_ref<T>(error);
        if (typed.is_some()) return typed;
        auto transfer = rstd::error::downcast_ref<Error>(error);
        if (transfer.is_some() && (*transfer)->context().is_some()) {
            auto context = rstd::error::downcast_ref<T>((*transfer)->context()->deref());
            if (context.is_some()) return context;
        }
        auto io = rstd::error::downcast_ref<rstd::io::Error>(error);
        if (io.is_some() && (*io)->get_ref().is_some()) {
            error = *(*io)->get_ref();
            continue;
        }
        auto next = error->source();
        if (next.is_none()) return None();
        error = *next;
    }
    return None();
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
auto tunnel_close_twice(rstd::async::io::Stream& stream) -> coro<empty> {
    for (unsigned count = 0; count < 2; ++count) {
        auto closed = co_await rstd::async::timeout(stream.wait_closed().into_future(),
                                                    Duration::from_secs(u64(2)));
        EXPECT_TRUE(closed.is_ok());
        if (closed.is_ok()) EXPECT_TRUE(closed->is_ok());
    }
    auto result = stream.take_close_result();
    if (result.is_some()) EXPECT_TRUE(result->is_ok());
    co_return empty {};
}
void tunnel_expect_broken_pipe(const rstd::io::Result<usize>& result) {
    EXPECT_TRUE(result.is_err());
    if (result.is_err())
        EXPECT_EQ(result.as_ref().unwrap_err().kind(),
                  rstd::io::ErrorKind { rstd::io::ErrorKind::BrokenPipe });
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
        EXPECT_TRUE((co_await a.wait_closed()).is_ok());
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
        auto phase = tunnel_cause<http::ConnectFailure>(
            rstd::dyn<rstd::error::Error>::from_ref(error).deref());
        EXPECT_TRUE(phase.is_some());
        if (phase.is_some()) {
            EXPECT_EQ((*phase)->stage, http::ConnectStage::ConnectHead);
            EXPECT_TRUE((*phase)->session_id != u64());
            EXPECT_TRUE((*phase)->stream_id != u32());
        }
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
        auto engine           = Engine::make(runtime.handle()).unwrap();
        auto client           = tunnel_client(engine);
        auto waiting          = client.open("stall.test:443"_Str).unwrap();
        auto ready            = client.open("ready.test:443"_Str).unwrap();
        auto connection       = (co_await ready.connected()).unwrap();
        auto stream           = rstd::move(connection).into_stream();
        auto waiting_progress = waiting.progress();
        EXPECT_EQ(waiting_progress.stage, http::ConnectStage::ConnectHead);
        EXPECT_TRUE(waiting_progress.session_id != u64());
        EXPECT_TRUE(waiting_progress.stream_id != u32());
        EXPECT_EQ(waiting_progress.session_id, ready.progress().session_id);
        EXPECT_EQ(ready.progress().stage, http::ConnectStage::TunnelIo);
        waiting.request_stop();
        EXPECT_TRUE((co_await waiting.close()).is_ok());
        auto result = co_await waiting.connected();
        EXPECT_TRUE(result.is_err());
        EXPECT_EQ((co_await stream.send_all(Bytes::copy_from_slice("still works"_bytes))).unwrap(),
                  usize(11));
        EXPECT_EQ((co_await tunnel_receive(stream, usize(11))).as_slice(), "still works"_bytes);
        EXPECT_TRUE((co_await stream.close()).is_ok());
        EXPECT_TRUE((co_await client.close()).is_ok());
        EXPECT_TRUE((co_await client.wait_closed()).is_ok());
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

TEST(HttpConnectDiagnostic, ContextMovePreservesErrorVariants) {
    for (bool canceled : array<bool, 2> { false, true }) {
        auto cause   = canceled ? Error::Canceled() : Error::Timeout();
        auto context = Box<rstd::dyn<rstd::error::Error>>::make(
            http::ConnectFailure { http::ConnectStage::ConnectHead, u64(7), u32(9) });
        auto annotated = rstd::move(cause).with_context(rstd::move(context));
        auto moved     = rstd::move(annotated);
        EXPECT_TRUE(canceled ? moved.is_Canceled() : moved.is_Timeout());
        auto diagnostic = tunnel_cause<http::ConnectFailure>(
            rstd::dyn<rstd::error::Error>::from_ref(moved).deref());
        EXPECT_TRUE(diagnostic.is_some());
        if (diagnostic.is_none()) continue;
        EXPECT_EQ((*diagnostic)->stage, http::ConnectStage::ConnectHead);
        EXPECT_EQ((*diagnostic)->session_id, u64(7));
        EXPECT_EQ((*diagnostic)->stream_id, u32(9));
        EXPECT_TRUE(rstd::dyn<rstd::error::Error>::from_ref(moved)->source().is_none());
    }
}
TEST(H2Tunnel, ConnectHeadTimeoutKeepsTimeoutVariantAndTypedPhase) {
    if (! ::getenv("LIFETCHTO_H2_PORT")) GTEST_SKIP();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        auto engine  = Engine::make(runtime.handle()).unwrap();
        auto client  = tunnel_client(engine, Duration::from_millis(u64(250)));
        auto waiting = client.open("stall.test:443"_Str).unwrap();
        auto ready   = client.open("ready.test:443"_Str).unwrap();
        auto sibling = (co_await ready.connected()).unwrap();
        EXPECT_EQ(waiting.progress().stage, http::ConnectStage::ConnectHead);
        auto result = co_await waiting.connected();
        EXPECT_TRUE(result.is_err());
        if (result.is_err()) {
            auto error = rstd::move(result).unwrap_err();
            EXPECT_TRUE(error.is_Timeout());
            auto phase = tunnel_cause<http::ConnectFailure>(
                rstd::dyn<rstd::error::Error>::from_ref(error).deref());
            EXPECT_TRUE(phase.is_some());
            if (phase.is_some()) {
                EXPECT_EQ((*phase)->stage, http::ConnectStage::ConnectHead);
                EXPECT_EQ((*phase)->session_id, waiting.progress().session_id);
                EXPECT_EQ((*phase)->stream_id, waiting.progress().stream_id);
            }
        }
        auto stream = rstd::move(sibling).into_stream();
        EXPECT_TRUE((co_await stream.close()).is_ok());
        EXPECT_TRUE((co_await waiting.close()).is_ok());
        EXPECT_TRUE((co_await client.close()).is_ok());
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(run());
}
TEST(H2Tunnel, StreamResetPreservesHttp2CodeAndLifecycleThroughIoSource) {
    if (! ::getenv("LIFETCHTO_H2_PORT")) GTEST_SKIP();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        auto engine     = Engine::make(runtime.handle()).unwrap();
        auto client     = tunnel_client(engine);
        auto pending    = client.open("reset-after.test:443"_Str).unwrap();
        auto connection = (co_await pending.connected()).unwrap();
        auto stream     = rstd::move(connection).into_stream();
        EXPECT_EQ((co_await stream.send(Bytes::copy_from_slice("reset"_bytes))).unwrap(), usize(5));
        auto received = co_await stream.recv();
        EXPECT_TRUE(received.is_err());
        if (received.is_err()) {
            auto error    = rstd::move(received).unwrap_err();
            auto owned    = rstd::dyn<rstd::error::Error>::from_ref(error).deref();
            auto phase    = tunnel_cause<http::ConnectFailure>(owned);
            auto protocol = tunnel_cause<lihttpto::h2::Error>(owned);
            EXPECT_TRUE(phase.is_some());
            EXPECT_TRUE(protocol.is_some());
            if (phase.is_some()) {
                EXPECT_EQ((*phase)->stage, http::ConnectStage::TunnelIo);
                EXPECT_EQ((*phase)->session_id, pending.progress().session_id);
                EXPECT_EQ((*phase)->stream_id, pending.progress().stream_id);
            }
            if (protocol.is_some()) {
                EXPECT_TRUE((*protocol)->is_Stream());
                if ((*protocol)->is_Stream()) {
                    EXPECT_EQ((*protocol)->as_Stream().id, pending.progress().stream_id);
                    EXPECT_EQ((*protocol)->as_Stream().code, u32(7));
                }
            }
        }
        EXPECT_TRUE((co_await stream.close()).is_ok());
        EXPECT_TRUE((co_await client.close()).is_ok());
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(run());
}

TEST(H2Tunnel, PendingProgressDistinguishesTlsFromHttp2Settings) {
    if (! ::getenv("LIFETCHTO_H2_TLS_STALL_PORT") || ! ::getenv("LIFETCHTO_H2_SETTINGS_STALL_PORT"))
        GTEST_SKIP();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        for (bool tls : array<bool, 2> { true, false }) {
            auto engine  = Engine::make(runtime.handle()).unwrap();
            auto client  = tunnel_client(engine,
                                         Duration::from_millis(u64(500)),
                                         tls ? "LIFETCHTO_H2_TLS_STALL_PORT"
                                             : "LIFETCHTO_H2_SETTINGS_STALL_PORT");
            auto pending = client.open("phase.test:443"_Str).unwrap();
            auto future  = pending.connected().into_future();
            Option<lifetchto::Result<http::TunnelConnection>> result;
            auto expected = tls ? http::ConnectStage::Tls : http::ConnectStage::Http2Settings;
            auto began    = rstd::time::Instant::now();
            while (pending.progress().stage != expected &&
                   began.elapsed() < Duration::from_millis(u64(250))) {
                auto ready = co_await rstd::async::timeout(rstd::future::by_ref(future),
                                                           Duration::from_millis(u64(10)));
                if (ready.is_ok()) {
                    result = Some(rstd::move(ready).unwrap());
                    break;
                }
            }
            auto progress = pending.progress();
            EXPECT_EQ(progress.stage, expected);
            EXPECT_TRUE(progress.session_id != u64());
            if (tls) EXPECT_EQ(progress.stream_id, u32());
            if (result.is_none()) result = Some(co_await rstd::future::by_ref(future));
            EXPECT_TRUE(result->is_err());
            if (result->is_err()) {
                auto error = rstd::move(*result).unwrap_err();
                if (! tls) EXPECT_TRUE(error.is_Timeout());
                auto phase = tunnel_cause<http::ConnectFailure>(
                    rstd::dyn<rstd::error::Error>::from_ref(error).deref());
                EXPECT_TRUE(phase.is_some());
                if (phase.is_some()) {
                    EXPECT_EQ((*phase)->stage, expected);
                    EXPECT_EQ((*phase)->session_id, progress.session_id);
                }
            }
            EXPECT_TRUE((co_await pending.close()).is_ok());
            auto closed = co_await client.close();
            EXPECT_EQ(closed.is_err(), tls);
            if (tls && closed.is_err()) {
                auto error = rstd::move(closed).unwrap_err();
                EXPECT_TRUE(error.is_Extension());
                if (error.is_Extension()) EXPECT_EQ(error.as_Extension().name.as_str(), "tls"_str);
            }
            EXPECT_EQ((co_await engine.close()).total_errors, tls ? usize(1) : usize());
        }
        co_return empty {};
    };
    runtime.block_on(run());
}

TEST(H2Tunnel, EndAndNoErrorResetInOneBatchDrainEveryBufferedFrameBeforeEof) {
    if (! ::getenv("LIFETCHTO_H2_PORT")) GTEST_SKIP();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        auto engine = Engine::make(runtime.handle()).unwrap();
        auto client = tunnel_client(
            engine, Duration::from_secs(u64(2)), "LIFETCHTO_H2_PORT", {}, Some(usize(1)));
        auto pending = client.open("clean-batch.test:443"_Str).unwrap();
        auto ready   = (co_await pending.connected()).unwrap();
        auto identity =
            String::make(*ready.head().headers.get_unique_text("x-connection"_str).unwrap());
        auto stream  = rstd::move(ready).into_stream();
        auto reading = stream.recv(usize(2)).into_future();
        auto cx      = rstd::task::Context { rstd::task::Waker::noop() };
        EXPECT_TRUE(rstd::future::poll(reading, cx).is_pending());
        EXPECT_EQ((co_await stream.send_all(Bytes::copy_from_slice("go"_bytes))).unwrap(),
                  usize(2));
        auto first = (co_await rstd::future::by_ref(reading)).unwrap();
        EXPECT_EQ(first.as_slice(), "fi"_bytes);

        // A released wire slot must not retire unread application bytes.
        auto sibling   = client.open("batch-sibling.test:443"_Str).unwrap();
        auto connected = (co_await sibling.connected()).unwrap();
        EXPECT_EQ(*connected.head().headers.get_unique_text("x-connection"_str).unwrap(),
                  identity.as_str());
        auto echo = rstd::move(connected).into_stream();
        EXPECT_TRUE((co_await stream.shutdown_write()).is_ok());
        EXPECT_TRUE((co_await stream.shutdown_write()).is_ok());
        EXPECT_TRUE((co_await stream.flush()).is_ok());
        tunnel_expect_broken_pipe(
            co_await stream.send(Bytes::copy_from_slice("not accepted"_bytes)));
        EXPECT_EQ((co_await echo.send_all(Bytes::copy_from_slice("alive"_bytes))).unwrap(),
                  usize(5));
        EXPECT_EQ((co_await tunnel_receive(echo, usize(5))).as_slice(), "alive"_bytes);
        auto    retained_bytes = engine.resources().usage().bytes;
        Vec<u8> tail;
        for (;;) {
            auto part = (co_await stream.recv(usize(2))).unwrap();
            if (part.is_empty()) break;
            tail.extend_from_slice(part.as_slice());
        }
        EXPECT_EQ(tail.as_slice(), "rst-second-last"_bytes);
        EXPECT_LT(engine.resources().usage().bytes, retained_bytes);
        EXPECT_TRUE((co_await stream.recv()).unwrap().is_empty());
        co_await tunnel_close_twice(stream);
        co_await tunnel_close_twice(echo);
        EXPECT_TRUE((co_await client.close()).is_ok());
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(run());
}

TEST(H2Tunnel, NoErrorResetAfterObservedEofKeepsEofAndClosesOnlyLocalWrite) {
    if (! ::getenv("LIFETCHTO_H2_PORT")) GTEST_SKIP();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        auto engine     = Engine::make(runtime.handle()).unwrap();
        auto client     = tunnel_client(engine);
        auto pending    = client.open("clean-split.test:443"_Str).unwrap();
        auto control    = client.open("control.test:443"_Str).unwrap();
        auto ready      = (co_await pending.connected()).unwrap();
        auto controlled = (co_await control.connected()).unwrap();
        EXPECT_EQ(*ready.head().headers.get_unique_text("x-connection"_str).unwrap(),
                  *controlled.head().headers.get_unique_text("x-connection"_str).unwrap());
        auto stream = rstd::move(ready).into_stream();
        auto echo   = rstd::move(controlled).into_stream();
        EXPECT_EQ((co_await stream.send_all(Bytes::copy_from_slice("go"_bytes))).unwrap(),
                  usize(2));
        EXPECT_EQ((co_await tunnel_receive(stream, usize(10))).as_slice(), "split-body"_bytes);
        EXPECT_TRUE((co_await stream.recv()).unwrap().is_empty());
        EXPECT_EQ(
            (co_await stream.send_all(Bytes::copy_from_slice("still writable"_bytes))).unwrap(),
            usize(14));
        EXPECT_EQ((co_await echo.send_all(Bytes::copy_from_slice("reset-split\n"_bytes))).unwrap(),
                  usize(12));
        EXPECT_EQ((co_await tunnel_receive(echo, usize(12))).as_slice(), "reset-split\n"_bytes);
        EXPECT_TRUE((co_await stream.recv()).unwrap().is_empty());
        tunnel_expect_broken_pipe(co_await stream.send(Bytes::copy_from_slice("refused"_bytes)));
        EXPECT_TRUE((co_await stream.flush()).is_ok());
        EXPECT_TRUE((co_await stream.shutdown_write()).is_ok());
        EXPECT_TRUE((co_await stream.shutdown_write()).is_ok());
        EXPECT_EQ((co_await echo.send_all(Bytes::copy_from_slice("sibling\n"_bytes))).unwrap(),
                  usize(8));
        EXPECT_EQ((co_await tunnel_receive(echo, usize(8))).as_slice(), "sibling\n"_bytes);
        co_await tunnel_close_twice(stream);
        co_await tunnel_close_twice(echo);
        EXPECT_TRUE((co_await client.close()).is_ok());
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(run());
}

TEST(H2Tunnel, NoErrorResetReturnsOnlyConfirmedUploadPrefixAndPreservesSibling) {
    if (! ::getenv("LIFETCHTO_H2_WINDOW_PORT")) GTEST_SKIP();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        auto engine = Engine::make(runtime.handle()).unwrap();
        auto client =
            tunnel_client(engine, Duration::from_secs(u64(2)), "LIFETCHTO_H2_WINDOW_PORT");
        auto pending   = client.open("clean-partial.test:443"_Str).unwrap();
        auto sibling   = client.open("partial-sibling.test:443"_Str).unwrap();
        auto ready     = (co_await pending.connected()).unwrap();
        auto connected = (co_await sibling.connected()).unwrap();
        EXPECT_EQ(*ready.head().headers.get_unique_text("x-connection"_str).unwrap(),
                  *connected.head().headers.get_unique_text("x-connection"_str).unwrap());
        auto    stream = rstd::move(ready).into_stream();
        auto    echo   = rstd::move(connected).into_stream();
        Vec<u8> payload;
        payload.resize(usize(4096), u8('p'));
        auto writing = stream.send(Bytes::from_vec(rstd::move(payload))).into_future();
        auto cx      = rstd::task::Context { rstd::task::Waker::noop() };
        EXPECT_TRUE(rstd::future::poll(writing, cx).is_pending());
        auto confirmed = (co_await rstd::future::by_ref(writing)).unwrap();
        EXPECT_EQ(confirmed, usize(1024));
        EXPECT_EQ((co_await tunnel_receive(stream, usize(4))).as_slice(), "done"_bytes);
        EXPECT_TRUE((co_await stream.recv()).unwrap().is_empty());
        tunnel_expect_broken_pipe(co_await stream.send(Bytes::copy_from_slice("tail"_bytes)));
        EXPECT_TRUE((co_await stream.shutdown_write()).is_ok());
        EXPECT_TRUE((co_await stream.shutdown_write()).is_ok());
        EXPECT_EQ((co_await echo.send_all(Bytes::copy_from_slice("survives"_bytes))).unwrap(),
                  usize(8));
        EXPECT_EQ((co_await tunnel_receive(echo, usize(8))).as_slice(), "survives"_bytes);
        co_await tunnel_close_twice(stream);
        co_await tunnel_close_twice(echo);
        EXPECT_TRUE((co_await client.close()).is_ok());
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(run());
}

TEST(H2Tunnel, CleanResetCloseWaiterCannotBypassPendingNativeWriteFence) {
    if (! ::getenv("LIFETCHTO_H2_WINDOW_PORT")) GTEST_SKIP();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        auto engine = Engine::make(runtime.handle()).unwrap();
        auto gate   = Arc<tunnel_test::Gate>::make();
        auto connector =
            Arc<rstd::dyn<http::TcpConnector>>::make(tunnel_test::Connector { gate.clone() });
        auto client    = tunnel_client(engine,
                                       Duration::from_secs(u64(2)),
                                       "LIFETCHTO_H2_WINDOW_PORT",
                                       Some(rstd::move(connector)));
        auto pending   = client.open("clean-partial.test:443"_Str).unwrap();
        auto sibling   = client.open("fence-sibling.test:443"_Str).unwrap();
        auto ready     = (co_await pending.connected()).unwrap();
        auto connected = (co_await sibling.connected()).unwrap();
        EXPECT_EQ(*ready.head().headers.get_unique_text("x-connection"_str).unwrap(),
                  *connected.head().headers.get_unique_text("x-connection"_str).unwrap());
        auto stream = rstd::move(ready).into_stream();
        auto echo   = rstd::move(connected).into_stream();
        EXPECT_EQ((co_await echo.send_all(Bytes::copy_from_slice("warm"_bytes))).unwrap(),
                  usize(4));
        EXPECT_EQ((co_await tunnel_receive(echo, usize(4))).as_slice(), "warm"_bytes);
        EXPECT_TRUE((co_await echo.flush()).is_ok());
        gate->fields.lock().unwrap()->armed = true;
        Vec<u8> payload;
        payload.resize(usize(4096), u8('p'));
        auto writing = stream.send(Bytes::from_vec(rstd::move(payload))).into_future();
        auto cx      = rstd::task::Context { rstd::task::Waker::noop() };
        EXPECT_TRUE(rstd::future::poll(writing, cx).is_pending());
        for (;;) {
            auto event = gate->changed.listen();
            if (gate->fields.lock().unwrap()->blocked) break;
            co_await rstd::move(event);
        }
        EXPECT_EQ((co_await tunnel_receive(stream, usize(4))).as_slice(), "done"_bytes);
        EXPECT_TRUE((co_await stream.recv()).unwrap().is_empty());
        auto write_state = rstd::future::poll(writing, cx);
        EXPECT_TRUE(write_state.is_pending());
        {
            auto closing = stream.close().into_future();
            EXPECT_TRUE(rstd::future::poll(closing, cx).is_pending());
        }
        auto closing     = stream.close().into_future();
        auto close_state = rstd::future::poll(closing, cx);
        EXPECT_TRUE(close_state.is_pending());
        gate->release();
        auto confirmed = write_state.is_ready() ? rstd::move(write_state).take()
                                                : co_await rstd::future::by_ref(writing);
        EXPECT_EQ(confirmed.unwrap(), usize(1024));
        if (close_state.is_pending())
            EXPECT_TRUE((co_await rstd::future::by_ref(closing)).is_ok());
        else
            EXPECT_TRUE(rstd::move(close_state).take().is_ok());
        co_await tunnel_close_twice(stream);
        EXPECT_FALSE(gate->fields.lock().unwrap()->stopped);
        EXPECT_EQ((co_await echo.send_all(Bytes::copy_from_slice("survives"_bytes))).unwrap(),
                  usize(8));
        EXPECT_EQ((co_await tunnel_receive(echo, usize(8))).as_slice(), "survives"_bytes);
        co_await tunnel_close_twice(echo);
        EXPECT_TRUE((co_await client.close()).is_ok());
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(run());
}

TEST(H2Tunnel, ResetWithoutEndAndNonzeroResetAfterEndKeepTypedFailures) {
    if (! ::getenv("LIFETCHTO_H2_PORT")) GTEST_SKIP();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        auto engine = Engine::make(runtime.handle()).unwrap();
        auto client = tunnel_client(engine);
        for (bool ended : array<bool, 2> { false, true }) {
            auto pending =
                client.open(ended ? "end-reset-error.test:443"_Str : "reset-zero.test:443"_Str)
                    .unwrap();
            auto stream = rstd::move((co_await pending.connected()).unwrap()).into_stream();
            EXPECT_EQ((co_await stream.send(Bytes::copy_from_slice("go"_bytes))).unwrap(),
                      usize(2));
            auto result = co_await stream.recv();
            EXPECT_TRUE(result.is_err());
            if (result.is_err()) {
                auto error    = rstd::move(result).unwrap_err();
                auto source   = rstd::dyn<rstd::error::Error>::from_ref(error).deref();
                auto phase    = tunnel_cause<http::ConnectFailure>(source);
                auto protocol = tunnel_cause<lihttpto::h2::Error>(source);
                EXPECT_TRUE(phase.is_some());
                EXPECT_TRUE(protocol.is_some());
                if (phase.is_some()) {
                    EXPECT_EQ((*phase)->stage, http::ConnectStage::TunnelIo);
                    EXPECT_EQ((*phase)->session_id, pending.progress().session_id);
                    EXPECT_EQ((*phase)->stream_id, pending.progress().stream_id);
                }
                if (protocol.is_some()) {
                    EXPECT_TRUE((*protocol)->is_Stream());
                    if ((*protocol)->is_Stream()) {
                        EXPECT_EQ((*protocol)->as_Stream().id, pending.progress().stream_id);
                        EXPECT_EQ((*protocol)->as_Stream().code, ended ? u32(7) : u32());
                    }
                }
            }
            EXPECT_TRUE((co_await stream.send(Bytes::copy_from_slice("refused"_bytes))).is_err());
            co_await tunnel_close_twice(stream);
        }
        EXPECT_TRUE((co_await client.close()).is_ok());
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(run());
}
