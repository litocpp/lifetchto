#include <rstd/test/gtest.hpp>
import lifetchto.core;
using namespace rstd::prelude;
using namespace rstd::literals;
namespace lf = lifetchto;
using lf::Arc;
using lf::coro;
using lf::Duration;
using lf::Bytes;
namespace
{
struct SessionCounts {
    usize active {}, released {};
    bool  closing {}, closed {};
};
struct StreamState;
struct PhysicalFields {
    SessionCounts                      counts;
    Vec<rstd::sync::Weak<StreamState>> streams;
};
struct PhysicalState {
    rstd::sync::Mutex<PhysicalFields> fields { PhysicalFields {} };
    lf::Event                         changed;
};
struct StreamFields {
    bool read_done {}, active {}, released {};
};
struct StreamState {
    lf::ByteQueue                   outgoing, incoming;
    rstd::sync::Mutex<StreamFields> fields { StreamFields {} };
    lf::Event                       changed;
    StreamState(lf::ByteQueue outgoing, lf::ByteQueue incoming)
        : outgoing(rstd::move(outgoing)), incoming(rstd::move(incoming)) {}
    void stop(lf::StopReason reason) {
        outgoing.fail(lf::stop_error(reason));
        incoming.fail(lf::stop_error(reason));
        changed.notify();
    }
};
struct StreamOwner {
    Arc<PhysicalState> physical;
    Arc<StreamState>   stream;
    void               request_stop(lf::StopReason reason) const { stream->stop(reason); }
    auto               release(lf::ReleaseContext) const -> coro<lf::Result<empty>> {
        co_await rstd::async::sleep(Duration::from_millis(u64(2)));
        stream->outgoing.release();
        stream->incoming.release();
        {
            auto p = physical->fields.lock().unwrap();
            auto s = stream->fields.lock().unwrap();
            if (! s->released) {
                s->released = true;
                ++p->counts.released;
                if (s->active) --p->counts.active;
            }
        }
        physical->changed.notify();
        co_return Ok(empty {});
    }
};
struct PhysicalOwner {
    Arc<PhysicalState> state;
    void               request_stop(lf::StopReason reason) const {
        Vec<Arc<StreamState>> streams;
        {
            auto f            = state->fields.lock().unwrap();
            f->counts.closing = true;
            for (const auto& weak : f->streams) {
                auto stream = weak.upgrade();
                if (stream) streams.push(rstd::move(stream));
            }
        }
        for (const auto& stream : streams) stream->stop(reason);
    }
    auto release(lf::ReleaseContext) const -> coro<lf::Result<empty>> {
        for (;;) {
            auto event = state->changed.listen();
            {
                auto f = state->fields.lock().unwrap();
                if (f->counts.active == usize()) {
                    f->counts.closed = true;
                    co_return Ok(empty {});
                }
            }
            co_await rstd::move(event);
        }
    }
};
struct Endpoint {
    Arc<StreamState> stream;
    auto             send(Bytes bytes) const -> coro<lf::Result<empty>> {
        return stream->outgoing.send(rstd::move(bytes));
    }
    auto next() const -> coro<lf::Result<Option<Bytes>>>;
    void finish() const { stream->outgoing.finish(); }
};
auto receive(Arc<StreamState> stream) -> coro<lf::Result<Option<Bytes>>> {
    auto bytes = co_await stream->incoming.next();
    if (bytes.is_ok() && bytes->is_none()) {
        auto f       = stream->fields.lock().unwrap();
        f->read_done = true;
        stream->changed.notify();
    }
    co_return rstd::move(bytes);
}
auto Endpoint::next() const -> coro<lf::Result<Option<Bytes>>> {
    return receive(stream.clone());
}
struct EchoState {
    rstd::sync::Mutex<Option<Arc<StreamState>>> stream { None<Arc<StreamState>>() };
};
struct EchoDriver {
    Arc<PhysicalState> physical;
    Arc<EchoState>     state { Arc<EchoState>::make() };
    auto               run(lf::TransferContext context, lf::OutputSender<Endpoint> output)
        -> coro<lf::ProtocolCompletion>;
    void request_stop(lf::StopReason reason) {
        Option<Arc<StreamState>> stream;
        {
            auto f = state->stream.lock().unwrap();
            if (f->is_some()) stream = Some((*f)->clone());
        }
        if (stream.is_some()) (*stream)->stop(reason);
    }
    auto release(lf::ReleaseContext) -> coro<lf::Result<empty>> { co_return Ok(empty {}); }
};
} // namespace
namespace rstd
{
template<>
struct Impl<lf::ManagedResource, StreamOwner> : ImplBase<StreamOwner> {
    void request_stop(lf::StopReason reason) const { this->self().request_stop(reason); }
    auto release(lf::ReleaseContext context) const { return this->self().release(context); }
};
template<>
struct Impl<lf::ManagedResource, PhysicalOwner> : ImplBase<PhysicalOwner> {
    void request_stop(lf::StopReason reason) const { this->self().request_stop(reason); }
    auto release(lf::ReleaseContext context) const { return this->self().release(context); }
};
} // namespace rstd
namespace
{
auto EchoDriver::run(lf::TransferContext context, lf::OutputSender<Endpoint> output)
    -> coro<lf::ProtocolCompletion> {
    auto out    = (co_await lf::ByteQueue::make(context.resources(), usize(8))).unwrap();
    auto in     = (co_await lf::ByteQueue::make(context.resources(), usize(8))).unwrap();
    auto stream = Arc<StreamState>::make(rstd::move(out), rstd::move(in));
    {
        auto f = state->stream.lock().unwrap();
        *f     = Some(stream.clone());
    }
    auto registration =
        context.resources().attach(empty {},
                                   Arc<rstd::dyn<lf::ManagedResource>>::make(
                                       StreamOwner { physical.clone(), stream.clone() }));
    if (registration.is_err()) {
        auto error = rstd::move(registration).unwrap_err();
        (void)co_await error.input.close();
        co_return lf::ProtocolCompletion { Err(rstd::move(error.error)),
                                           lf::RemoteOutcome::NotDispatched };
    }
    auto lease = rstd::move(registration).unwrap();
    {
        auto p = physical->fields.lock().unwrap();
        auto s = stream->fields.lock().unwrap();
        if (! p->counts.closing && ! s->released) {
            ++p->counts.active;
            s->active = true;
            p->streams.push(stream.downgrade());
        }
    }
    auto published = output.publish(Endpoint { stream.clone() });
    if (published.is_err())
        co_return lf::ProtocolCompletion { Err(rstd::move(published).unwrap_err()),
                                           lf::RemoteOutcome::Unknown };
    for (;;) {
        auto bytes = co_await stream->outgoing.next();
        if (bytes.is_err())
            co_return lf::ProtocolCompletion { Err(rstd::move(bytes).unwrap_err()),
                                               lf::RemoteOutcome::Unknown };
        if (bytes->is_none()) break;
        auto sent = co_await stream->incoming.send(bytes->take().unwrap());
        if (sent.is_err())
            co_return lf::ProtocolCompletion { Err(rstd::move(sent).unwrap_err()),
                                               lf::RemoteOutcome::Unknown };
    }
    stream->incoming.finish();
    for (;;) {
        auto event = stream->changed.listen();
        {
            auto f = stream->fields.lock().unwrap();
            if (f->read_done) break;
        }
        auto ready = co_await rstd::async::select(rstd::move(event),
                                                  context.cancellation().stopped().into_future());
        if (ready.is_right())
            co_return lf::ProtocolCompletion {
                Err(lf::stop_error(rstd::move(ready).unwrap_right())), lf::RemoteOutcome::Unknown
            };
    }
    co_return lf::ProtocolCompletion { Ok(empty {}), lf::RemoteOutcome::Confirmed };
}
} // namespace
TEST(TransferCore, DuplexOutputRequiresExplicitFinishAndRemainsUnderDeadline) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        auto engine   = lf::Engine::make(runtime.handle()).unwrap();
        auto physical = Arc<PhysicalState>::make();
        auto owner    = engine.resources()
                            .attach(empty {},
                                    Arc<rstd::dyn<lf::ManagedResource>>::make(
                                        PhysicalOwner { physical.clone() }))
                            .unwrap();
        auto transfer =
            engine
                .submit(lf::PreparedOperation<Endpoint>::make(EchoDriver { physical.clone() }),
                        { .deadline = Some(lf::Instant::now() + Duration::from_secs(u64(1))) })
                .unwrap();
        auto endpoint = (co_await transfer.output()).unwrap();
        EXPECT_TRUE((co_await endpoint.send(Bytes::copy_from_slice("abc"_str.as_bytes()))).is_ok());
        auto echo = (co_await endpoint.next()).unwrap().unwrap();
        EXPECT_EQ(echo.as_slice(), "abc"_str.as_bytes());
        EXPECT_EQ(transfer.control().snapshot().phase, lf::TransferPhase::Running);
        endpoint.finish();
        EXPECT_TRUE((co_await endpoint.next()).unwrap().is_none());
        auto report = co_await transfer.control().wait();
        EXPECT_TRUE(report->logical.result.is_ok());
        EXPECT_EQ(report->logical.remote, lf::RemoteOutcome::Confirmed);
        EXPECT_TRUE((co_await engine.close()).is_ok());
        EXPECT_TRUE(physical->fields.lock().unwrap()->counts.closed);
        co_return empty {};
    };
    runtime.block_on(run());
}
TEST(TransferCore, CancelOneStreamKeepsSiblingAndPhysicalSessionAlive) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        auto engine   = lf::Engine::make(runtime.handle()).unwrap();
        auto physical = Arc<PhysicalState>::make();
        auto owner    = engine.resources()
                            .attach(empty {},
                                    Arc<rstd::dyn<lf::ManagedResource>>::make(
                                        PhysicalOwner { physical.clone() }))
                            .unwrap();
        auto first =
            engine.submit(lf::PreparedOperation<Endpoint>::make(EchoDriver { physical.clone() }))
                .unwrap();
        auto second =
            engine.submit(lf::PreparedOperation<Endpoint>::make(EchoDriver { physical.clone() }))
                .unwrap();
        auto a = (co_await first.output()).unwrap(), b = (co_await second.output()).unwrap();
        first.control().cancel();
        auto stopped = co_await first.control().wait();
        EXPECT_EQ(stopped->logical.remote, lf::RemoteOutcome::Unknown);
        EXPECT_TRUE(stopped->logical.result.is_err());
        EXPECT_FALSE(physical->fields.lock().unwrap()->counts.closed);
        EXPECT_TRUE((co_await b.send(Bytes::copy_from_slice("still"_str.as_bytes()))).is_ok());
        auto echo = (co_await b.next()).unwrap().unwrap();
        EXPECT_EQ(echo.as_slice(), "still"_str.as_bytes());
        b.finish();
        EXPECT_TRUE((co_await b.next()).unwrap().is_none());
        EXPECT_TRUE((co_await second.control().wait())->logical.result.is_ok());
        EXPECT_EQ(physical->fields.lock().unwrap()->counts.released, usize(2));
        EXPECT_TRUE((co_await engine.close()).is_ok());
        EXPECT_TRUE(physical->fields.lock().unwrap()->counts.closed);
        co_return empty {};
    };
    runtime.block_on(run());
}
TEST(TransferCore, DeadlineAfterDuplexOutputClosesLeaseWithoutFinishing) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        auto engine   = lf::Engine::make(runtime.handle()).unwrap();
        auto physical = Arc<PhysicalState>::make();
        auto owner    = engine.resources()
                            .attach(empty {},
                                    Arc<rstd::dyn<lf::ManagedResource>>::make(
                                        PhysicalOwner { physical.clone() }))
                            .unwrap();
        auto transfer =
            engine
                .submit(lf::PreparedOperation<Endpoint>::make(EchoDriver { physical.clone() }),
                        { .deadline = Some(lf::Instant::now() + Duration::from_millis(u64(15))) })
                .unwrap();
        auto endpoint = (co_await transfer.output()).unwrap();
        auto report   = co_await transfer.control().wait();
        EXPECT_TRUE(report->logical.result.as_ref().unwrap_err().is_Timeout());
        EXPECT_EQ(report->logical.remote, lf::RemoteOutcome::Unknown);
        EXPECT_EQ(report->stop_requested, Some(lf::StopReason::Deadline));
        EXPECT_TRUE((co_await endpoint.next()).is_err());
        EXPECT_EQ(physical->fields.lock().unwrap()->counts.released, usize(1));
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(run());
}
