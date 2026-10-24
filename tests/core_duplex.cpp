#include <rstd/test/gtest.hpp>
import lifetchto.core;
using namespace rstd::prelude;
using namespace rstd::literals;
namespace lf = lifetchto;
using lf::Arc;
using lf::coro;
namespace
{
struct Fields {
    Option<lf::DuplexChannel> channel;
    Option<lf::TaskControl>   writer;
    usize                     exits {};
};
using State = Arc<rstd::sync::Mutex<Fields>>;
struct Echo {
    lf::DuplexChannel channel;
    auto              run(lf::Cancellation) const -> coro<lf::Result<empty>> {
        for (;;) {
            auto next = co_await channel.next_write();
            if (next.is_err()) co_return Ok(empty {});
            auto command = rstd::move(next).unwrap();
            if (command.is_half_close()) {
                channel.end_input();
                command.complete(Ok(usize()));
                continue;
            }
            co_await rstd::async::sleep(lf::Duration::from_millis(u64(10)));
            auto queued = co_await channel.receive(command.bytes().clone());
            if (queued.is_err())
                command.complete(Err(rstd::move(queued).unwrap_err()));
            else
                command.complete(Ok(command.bytes().len()));
        }
    }
    void request_stop(lf::StopReason reason) const { channel.request_stop(reason); }
};
struct Driver {
    State state;
    auto  run(lf::TransferContext context, lf::OutputSender<lf::DuplexStream> output) const
        -> coro<lf::ProtocolCompletion> {
        auto channel = co_await lf::DuplexChannel::make(context.clone(), usize(8));
        if (channel.is_err())
            co_return lf::ProtocolCompletion { Err(rstd::move(channel).unwrap_err()),
                                               lf::RemoteOutcome::NotDispatched };
        auto writer = context.resources().spawn(Echo { channel->clone() });
        if (writer.is_err())
            co_return lf::ProtocolCompletion { Err(rstd::move(writer).unwrap_err().error),
                                               lf::RemoteOutcome::NotDispatched };
        {
            auto f     = state->lock().unwrap();
            f->channel = Some(channel->clone());
            f->writer  = Some(rstd::move(writer).unwrap());
        }
        auto sent = output.publish(channel->take_stream().unwrap());
        if (sent.is_err()) channel->request_stop();
        co_return lf::ProtocolCompletion { co_await channel->wait_finished(),
                                           lf::RemoteOutcome::Unknown };
    }
    void request_stop(lf::StopReason reason) const {
        auto f = state->lock().unwrap();
        if (f->channel.is_some()) f->channel->request_stop(reason);
    }
    auto release(lf::ReleaseContext) const -> coro<lf::Result<empty>> {
        Option<lf::TaskControl>   writer;
        Option<lf::DuplexChannel> channel;
        {
            auto f = state->lock().unwrap();
            if (f->channel.is_some()) channel = Some(f->channel->clone());
            writer = f->writer.take();
        }
        if (channel.is_some()) channel->request_stop(lf::StopReason::OwnerClosed);
        if (writer.is_some()) (void)co_await writer->wait();
        co_await rstd::async::sleep(lf::Duration::from_millis(u64(20)));
        if (channel.is_some()) channel->release_buffers();
        ++state->lock().unwrap()->exits;
        co_return Ok(empty {});
    }
};
} // namespace
TEST(DuplexChannel, AcknowledgesWritesAndRequiresFinishAfterEof) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        auto engine = lf::Engine::make(runtime.handle()).unwrap();
        auto state  = State::make(Fields {});
        auto transfer =
            engine.submit(lf::PreparedOperation<lf::DuplexStream>::make(Driver { state.clone() }))
                .unwrap();
        auto stream = (co_await transfer.output()).unwrap();
        EXPECT_TRUE((co_await stream.write(lf::Bytes::copy_from_slice("too-large"_str.as_bytes())))
                        .unwrap_err()
                        .is_TooLarge());
        auto writing =
            stream.write(lf::Bytes::copy_from_slice("echo"_str.as_bytes())).into_future();
        auto pending = co_await rstd::async::select(
            rstd::future::by_ref(writing), rstd::async::sleep(lf::Duration::from_millis(u64(1))));
        EXPECT_TRUE(pending.is_right());
        EXPECT_TRUE((co_await stream.write({})).unwrap_err().is_Busy());
        EXPECT_EQ((co_await rstd::move(writing)).unwrap(), usize(4));
        EXPECT_EQ((**(co_await stream.next())).as_slice(), "echo"_str.as_bytes());
        EXPECT_TRUE((co_await stream.close_input()).is_ok());
        EXPECT_TRUE((co_await stream.next()).unwrap().is_none());
        EXPECT_TRUE((co_await stream.write({})).unwrap_err().is_Closed());
        {
            auto waiting = transfer.control().wait().into_future();
            auto eof =
                co_await rstd::async::select(rstd::future::by_ref(waiting),
                                             rstd::async::sleep(lf::Duration::from_millis(u64(1))));
            EXPECT_TRUE(eof.is_right());
        }
        EXPECT_TRUE(stream.finish().is_ok());
        {
            auto waiting = transfer.control().wait().into_future();
            auto exiting =
                co_await rstd::async::select(rstd::future::by_ref(waiting),
                                             rstd::async::sleep(lf::Duration::from_millis(u64(1))));
            EXPECT_TRUE(exiting.is_right());
        }
        EXPECT_TRUE((co_await stream.completion().wait())->logical.result.is_ok());
        EXPECT_EQ(state->lock().unwrap()->exits, usize(1));
        EXPECT_TRUE((co_await stream.completion().wait())->cleanup.is_ok());
        EXPECT_EQ(engine.resources().usage().bytes, usize());
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(task());
}
TEST(DuplexChannel, DeadlineCoversOutputAndWaitsForRelease) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        auto                engine = lf::Engine::make(runtime.handle()).unwrap();
        auto                state  = State::make(Fields {});
        lf::TransferOptions options;
        options.deadline = Some(lf::Instant::now() + lf::Duration::from_millis(u64(20)));
        auto transfer =
            engine
                .submit(lf::PreparedOperation<lf::DuplexStream>::make(Driver { state.clone() }),
                        options)
                .unwrap();
        auto stream = (co_await transfer.output()).unwrap();
        auto read   = co_await stream.next();
        EXPECT_TRUE(read.unwrap_err().is_Timeout());
        auto report = co_await stream.completion().wait();
        EXPECT_TRUE(report->logical.result.as_ref().unwrap_err().is_Timeout());
        EXPECT_EQ(state->lock().unwrap()->exits, usize(1));
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(task());
}
TEST(DuplexChannel, CanceledReadStopsItsNativeOwnerAndEnforcesOneReader) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        auto engine = lf::Engine::make(runtime.handle()).unwrap();
        auto state  = State::make(Fields {});
        auto transfer =
            engine.submit(lf::PreparedOperation<lf::DuplexStream>::make(Driver { state.clone() }))
                .unwrap();
        auto stream = (co_await transfer.output()).unwrap();
        {
            auto reading = stream.next().into_future();
            auto waiting =
                co_await rstd::async::select(rstd::future::by_ref(reading),
                                             rstd::async::sleep(lf::Duration::from_millis(u64(1))));
            EXPECT_TRUE(waiting.is_right());
            EXPECT_TRUE((co_await stream.next()).unwrap_err().is_Busy());
            EXPECT_TRUE(stream.finish().unwrap_err().is_Busy());
        }
        EXPECT_TRUE((co_await stream.completion().wait())
                        ->logical.result.as_ref()
                        .unwrap_err()
                        .is_Canceled());
        EXPECT_EQ(state->lock().unwrap()->exits, usize(1));
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(task());
}
TEST(DuplexChannel, BackpressureExpiresAcknowledgementAndReleasesBuffers) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        auto                engine = lf::Engine::make(runtime.handle()).unwrap();
        auto                state  = State::make(Fields {});
        lf::TransferOptions options;
        options.deadline = Some(lf::Instant::now() + lf::Duration::from_millis(u64(50)));
        auto transfer =
            engine
                .submit(lf::PreparedOperation<lf::DuplexStream>::make(Driver { state.clone() }),
                        options)
                .unwrap();
        auto stream = (co_await transfer.output()).unwrap();
        EXPECT_EQ(
            (co_await stream.write(lf::Bytes::copy_from_slice("12345678"_str.as_bytes()))).unwrap(),
            usize(8));
        auto blocked = co_await stream.write(lf::Bytes::copy_from_slice("x"_str.as_bytes()));
        EXPECT_TRUE(blocked.is_err());
        EXPECT_TRUE((co_await stream.completion().wait())
                        ->logical.result.as_ref()
                        .unwrap_err()
                        .is_Timeout());
        EXPECT_EQ(state->lock().unwrap()->exits, usize(1));
        EXPECT_EQ(engine.resources().usage().bytes, usize());
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(task());
}
