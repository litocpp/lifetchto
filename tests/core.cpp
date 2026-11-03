#include <rstd/test/gtest.hpp>
import lifetchto.core;
using namespace rstd::prelude;
using namespace rstd::literals;
namespace lf = lifetchto;
using lf::coro;
using lf::Arc;
using lf::Duration;

struct OwnedConnectCause {
    int* drops;
    int  value;
    OwnedConnectCause(int& drops, int value): drops(&drops), value(value) {}
    OwnedConnectCause(const OwnedConnectCause&) = delete;
    OwnedConnectCause(OwnedConnectCause&& other) noexcept
        : drops(rstd::exchange(other.drops, nullptr)), value(other.value) {}
    ~OwnedConnectCause() {
        if (drops) ++*drops;
    }
};

namespace
{
struct Counts {
    usize runs {}, releases {}, closed {}, commits {};
    bool  stopped {};
};
using Stats = Arc<rstd::sync::Mutex<Counts>>;
struct Command {
    Stats    stats;
    Duration work { Duration::from_millis(u64(20)) };
    Duration cleanup { Duration::from_millis(u64(10)) };
    bool     reject {};
    auto     run(lf::TransferContext context, lf::OutputSender<String> output)
        -> coro<lf::ProtocolCompletion> {
        {
            auto f = stats->lock().unwrap();
            ++f->runs;
        }
        auto sent = output.publish(String::make("accepted locally"_str));
        if (sent.is_err())
            co_return lf::ProtocolCompletion { Err(rstd::move(sent).unwrap_err()),
                                               lf::RemoteOutcome::NotDispatched };
        context.progress(Some(u64(3)), None<u64>(), None<u64>());
        auto completed = co_await context.delay(work);
        if (completed.is_err())
            co_return lf::ProtocolCompletion { Err(rstd::move(completed).unwrap_err()),
                                               lf::RemoteOutcome::Unknown };
        if (reject)
            co_return lf::ProtocolCompletion { Err(lf::Error::BodySource(
                                                   String::make("peer rejected commit"_str))),
                                               lf::RemoteOutcome::Confirmed };
        {
            auto f = stats->lock().unwrap();
            ++f->commits;
        }
        co_return lf::ProtocolCompletion { Ok(empty {}), lf::RemoteOutcome::Confirmed };
    }
    void request_stop(lf::StopReason) {
        auto f     = stats->lock().unwrap();
        f->stopped = true;
    }
    auto release(lf::ReleaseContext) -> coro<lf::Result<empty>> {
        {
            auto f = stats->lock().unwrap();
            ++f->releases;
        }
        co_await rstd::async::sleep(cleanup);
        {
            auto f = stats->lock().unwrap();
            ++f->closed;
        }
        co_return Ok(empty {});
    }
};
struct Channel {
    Stats stats;
    void  request_stop(lf::StopReason) const {
        auto f     = stats->lock().unwrap();
        f->stopped = true;
    }
    auto release(lf::ReleaseContext) const -> coro<lf::Result<empty>> {
        {
            auto f = stats->lock().unwrap();
            ++f->releases;
        }
        co_await rstd::async::sleep(Duration::from_millis(u64(10)));
        {
            auto f = stats->lock().unwrap();
            ++f->closed;
        }
        co_return Ok(empty {});
    }
};
} // namespace
namespace rstd
{
template<>
struct Impl<fmt::Display, OwnedConnectCause> : ImplBase<OwnedConnectCause> {
    auto fmt(fmt::Formatter& f) const -> bool {
        return f.write_fmt(fmt::Arguments::make("connect cause {}", this->self().value));
    }
};
template<>
struct Impl<fmt::Debug, OwnedConnectCause> : Impl<fmt::Display, OwnedConnectCause> {};
template<>
struct Impl<error::Error, OwnedConnectCause> : DefaultInImpl<error::Error, OwnedConnectCause> {};

template<>
struct Impl<lf::ManagedResource, Channel> : ImplBase<Channel> {
    void request_stop(lf::StopReason reason) const { this->self().request_stop(reason); }
    auto release(lf::ReleaseContext context) const { return this->self().release(context); }
};
} // namespace rstd
namespace
{
struct TwoChannels {
    Stats first, second;
    auto run(lf::TransferContext context, lf::OutputSender<empty>) -> coro<lf::ProtocolCompletion> {
        auto a = context.resources()
                     .attach(usize(1),
                             Arc<rstd::dyn<lf::ManagedResource>>::make(Channel { first.clone() }))
                     .unwrap();
        auto b = context.resources()
                     .attach(usize(2),
                             Arc<rstd::dyn<lf::ManagedResource>>::make(Channel { second.clone() }))
                     .unwrap();
        auto step = co_await context.delay(Duration::from_millis(u64(1)));
        co_return lf::ProtocolCompletion { Err(lf::Error::BodySource(
                                               String::make("second channel setup failed"_str))),
                                           lf::RemoteOutcome::NotDispatched };
    }
    void request_stop(lf::StopReason) {}
    auto release(lf::ReleaseContext) -> coro<lf::Result<empty>> { co_return Ok(empty {}); }
};
struct ChildAttempt {
    auto run(lf::TransferContext context, lf::OutputSender<empty> output)
        -> coro<lf::ProtocolCompletion> {
        auto attempt = context.begin_attempt();
        if (attempt.is_err())
            co_return lf::ProtocolCompletion { Err(rstd::move(attempt).unwrap_err()),
                                               lf::RemoteOutcome::NotDispatched };
        co_return lf::ProtocolCompletion { output.publish(empty {}), lf::RemoteOutcome::Confirmed };
    }
    void request_stop(lf::StopReason) {}
    auto release(lf::ReleaseContext) -> coro<lf::Result<empty>> { co_return Ok(empty {}); }
};
struct ParentAttempt {
    auto run(lf::TransferContext context, lf::OutputSender<usize> output)
        -> coro<lf::ProtocolCompletion> {
        usize completed {};
        for (int i = 0; i < 3; ++i) {
            auto child =
                context.submit(lf::PreparedOperation<empty>::make(ChildAttempt {})).unwrap();
            auto value  = co_await child.output();
            auto report = co_await child.control().wait();
            if (report->logical.result.is_err()) break;
            ++completed;
        }
        co_return lf::ProtocolCompletion { output.publish(completed),
                                           lf::RemoteOutcome::Confirmed };
    }
    void request_stop(lf::StopReason) {}
    auto release(lf::ReleaseContext) -> coro<lf::Result<empty>> { co_return Ok(empty {}); }
};
} // namespace
TEST(FailureReport, FormatsMessageAndDebugSeparately) {
    const lf::FailureReport report { "request failed"_Str, "transport diagnostic"_Str };
    EXPECT_EQ(rstd::format("{}", report), "request failed"_str);
    EXPECT_EQ(rstd::format("{:?}", report), "transport diagnostic"_str);
}

TEST(FailureReport, PreservesEmptyFields) {
    const lf::FailureReport empty_message { ""_Str, "transport diagnostic"_Str };
    EXPECT_EQ(rstd::format("{}", empty_message), ""_str);
    EXPECT_EQ(rstd::format("{:?}", empty_message), "transport diagnostic"_str);
    const lf::FailureReport empty_debug { "request failed"_Str, ""_Str };
    EXPECT_EQ(rstd::format("{}", empty_debug), "request failed"_str);
    EXPECT_EQ(rstd::format("{:?}", empty_debug), ""_str);
}

TEST(TransferError, ConnectMoveKeepsOwnedIoPayloadAlive) {
    int drops {};
    {
        auto copy = [&] {
            auto original = lf::Error::Connect(rstd::io::Error::with_source(
                rstd::io::ErrorKind { rstd::io::ErrorKind::TimedOut },
                Box<rstd::dyn<rstd::error::Error>>::make(OwnedConnectCause(drops, 41))));
            return original;
        }();
        EXPECT_EQ(drops, 0);
        auto source = rstd::dyn<rstd::error::Error>::from_ref(copy)->source().unwrap();
        auto io     = rstd::error::downcast_ref<rstd::io::Error>(source);
        ASSERT_TRUE(io.is_some());
        EXPECT_EQ((*io)->kind(), rstd::io::ErrorKind { rstd::io::ErrorKind::TimedOut });
        EXPECT_TRUE(rstd::dyn<rstd::error::Error>::from_ref(**io)->source().is_none());
        auto payload = rstd::error::downcast_ref<OwnedConnectCause>((*io)->get_ref().unwrap());
        ASSERT_TRUE(payload.is_some());
        EXPECT_EQ((*payload)->value, 41);
        EXPECT_EQ(rstd::format("{}", copy), "connect cause 41"_str);
        auto retained = rstd::move(copy);
        copy          = lf::Error::Closed();
        EXPECT_EQ(drops, 0);
        EXPECT_EQ(rstd::format("{}", retained), "connect cause 41"_str);
    }
    EXPECT_EQ(drops, 1);
}

TEST(TransferError, ConnectMoveKeepsNativeCode) {
    auto copy = [] {
        auto original = lf::Error::Connect(rstd::io::Error::from_raw_os_error(i32::MIN));
        return original;
    }();
    EXPECT_EQ(copy.as_Connect().cause.raw_os_error().unwrap(), i32::MIN);
    auto source = rstd::dyn<rstd::error::Error>::from_ref(copy)->source().unwrap();
    auto io     = rstd::error::downcast_ref<rstd::io::Error>(source);
    ASSERT_TRUE(io.is_some());
    EXPECT_EQ((*io)->raw_os_error().unwrap(), i32::MIN);
    EXPECT_TRUE((*io)->get_ref().is_none());
}

struct DiagnosticFailure {
    int* drops;
    bool publish;
    auto run(lf::TransferContext, lf::OutputSender<empty> output) -> coro<lf::ProtocolCompletion> {
        if (publish) output.publish(empty {}).unwrap();
        co_return lf::ProtocolCompletion {
            Err(lf::Error::Connect(rstd::io::Error::with_source(
                rstd::io::ErrorKind { rstd::io::ErrorKind::TimedOut },
                Box<rstd::dyn<rstd::error::Error>>::make(OwnedConnectCause(*drops, 73))))),
            lf::RemoteOutcome::Unknown
        };
    }
    void request_stop(lf::StopReason) {}
    auto release(lf::ReleaseContext) -> coro<lf::Result<empty>> {
        co_return Err(lf::Error::BodySource("cleanup failed"_Str));
    }
};

struct OwnedCloseFailure {
    int* drops;
    int* releases;
    auto run(lf::TransferContext, lf::OutputSender<empty>) -> coro<lf::ProtocolCompletion> {
        co_return lf::ProtocolCompletion { Err(lf::Error::Closed()),
                                           lf::RemoteOutcome::NotDispatched };
    }
    void request_stop(lf::StopReason) const {}
    auto release(lf::ReleaseContext) const -> coro<lf::Result<empty>> {
        ++*releases;
        co_await rstd::async::sleep(Duration::from_millis(u64(10)));
        co_return Err(lf::Error::Connect(rstd::io::Error::with_source(
            rstd::io::ErrorKind { rstd::io::ErrorKind::BrokenPipe },
            Box<rstd::dyn<rstd::error::Error>>::make(OwnedConnectCause(*drops, 52)))));
    }
};
namespace rstd
{
template<>
struct Impl<lf::ManagedResource, OwnedCloseFailure> : ImplBase<OwnedCloseFailure> {
    void request_stop(lf::StopReason reason) const { this->self().request_stop(reason); }
    auto release(lf::ReleaseContext context) const { return this->self().release(context); }
};
} // namespace rstd

template<class Owner>
auto verify_owned_close(Owner& owner, int& drops, int& releases, bool explicit_take)
    -> coro<empty> {
    EXPECT_TRUE(owner.take_close_result().is_none());
    auto interrupted = co_await rstd::async::select(
        owner.close().into_future(), rstd::async::sleep(Duration::from_millis(u64(1))));
    EXPECT_TRUE(interrupted.is_right());
    EXPECT_TRUE(owner.take_close_result().is_none());
    auto first  = owner.wait_closed().into_future();
    auto second = owner.wait_closed().into_future();
    auto ready =
        co_await rstd::async::select(rstd::future::by_ref(first), rstd::future::by_ref(second));
    lf::CleanupReport report;
    if (ready.is_left()) {
        report = rstd::move(ready).unwrap_left();
        EXPECT_TRUE((co_await rstd::move(second)).is_err());
    } else {
        report = rstd::move(ready).unwrap_right();
        EXPECT_TRUE((co_await rstd::move(first)).is_err());
    }
    EXPECT_TRUE(report.is_err());
    EXPECT_TRUE((co_await owner.wait_closed()).is_err());
    EXPECT_EQ(report.errors[usize()].message.as_str(), "connect cause 52"_str);
    auto result = explicit_take ? owner.take_close_result() : Some(co_await owner.close());
    EXPECT_TRUE(result.is_some());
    EXPECT_TRUE(result->is_err());
    EXPECT_TRUE(owner.take_close_result().is_none());
    auto source = rstd::dyn<rstd::error::Error>::from_ref(result->as_ref().unwrap_err())->source();
    auto native = rstd::error::downcast_ref<rstd::io::Error>(source.unwrap()).unwrap();
    EXPECT_EQ(native->kind(), rstd::io::ErrorKind { rstd::io::ErrorKind::BrokenPipe });
    auto payload =
        rstd::error::downcast_ref<OwnedConnectCause>(native->get_ref().unwrap()).unwrap();
    EXPECT_EQ(payload->value, 52);
    EXPECT_EQ(drops, 0);
    result = None();
    EXPECT_EQ(drops, 1);
    EXPECT_TRUE((co_await owner.wait_closed()).is_err());
    EXPECT_EQ(report.errors[usize()].debug.as_str(), "connect cause 52"_str);
    EXPECT_TRUE((co_await owner.close()).unwrap_err().is_Contract());
    EXPECT_EQ(releases, 1);
    co_return empty {};
}

TEST(TransferCore, PreparedCloseObserversDoNotConsumeOwnedFailure) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    int  drops {}, releases {};
    auto owner = lf::PreparedOperation<empty>::make(OwnedCloseFailure { &drops, &releases });
    runtime.block_on(verify_owned_close(owner, drops, releases, true));
}

TEST(TransferCore, UnregisteredCloseObserversDoNotConsumeOwnedFailure) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    int  drops {}, releases {};
    auto owner = lf::ResourceRegistration<empty>(
        empty {},
        Arc<rstd::dyn<lf::ManagedResource>>::make(OwnedCloseFailure { &drops, &releases }));
    runtime.block_on(verify_owned_close(owner, drops, releases, false));
}

struct FailingTask {
    int* drops;
    auto run(lf::Cancellation) -> coro<lf::Result<empty>> {
        co_await rstd::async::sleep(Duration::from_millis(u64(10)));
        co_return Err(lf::Error::Connect(rstd::io::Error::with_source(
            rstd::io::ErrorKind { rstd::io::ErrorKind::BrokenPipe },
            Box<rstd::dyn<rstd::error::Error>>::make(OwnedConnectCause(*drops, 63)))));
    }
};

TEST(TransferCore, TaskObserversRetainDiagnosticsWithoutRetainingFailure) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    int  drops {};
    auto run = [&]() -> coro<empty> {
        Option<lf::TaskControl> observer;
        {
            auto engine      = lf::Engine::make(runtime.handle()).unwrap();
            auto task        = engine.resources().spawn(FailingTask { &drops }).unwrap();
            observer         = Some(task.clone());
            auto interrupted = co_await rstd::async::select(
                task.wait().into_future(), rstd::async::sleep(Duration::from_millis(u64(1))));
            EXPECT_TRUE(interrupted.is_right());
            auto first  = co_await task.wait();
            auto second = co_await observer->wait();
            EXPECT_TRUE(first.is_err());
            EXPECT_TRUE(second.is_err());
            EXPECT_EQ(first.errors[usize()].message.as_str(), "connect cause 63"_str);
            EXPECT_EQ(second.errors[usize()].debug.as_str(), "connect cause 63"_str);
            auto closed = co_await engine.close();
            EXPECT_EQ(closed.total_errors, usize(1));
            auto errors = engine.take_close_errors();
            EXPECT_EQ(errors.len(), usize(1));
            auto source  = rstd::dyn<rstd::error::Error>::from_ref(errors[usize()])->source();
            auto native  = rstd::error::downcast_ref<rstd::io::Error>(source.unwrap()).unwrap();
            auto payload = rstd::error::downcast_ref<OwnedConnectCause>(native->get_ref().unwrap());
            EXPECT_EQ(payload.unwrap()->value, 63);
            EXPECT_EQ(drops, 0);
            errors.clear();
            EXPECT_EQ(drops, 1);
            EXPECT_TRUE(engine.take_close_errors().is_empty());
            EXPECT_EQ((co_await engine.close()).total_errors, usize(1));
        }
        EXPECT_EQ(drops, 1);
        EXPECT_TRUE(observer->is_finished());
        observer->cancel();
        auto retained = co_await observer->wait();
        EXPECT_TRUE(retained.is_err());
        EXPECT_EQ(retained.errors[usize()].message.as_str(), "connect cause 63"_str);
        co_return empty {};
    };
    runtime.block_on(run());
    EXPECT_EQ(drops, 1);
}

TEST(TransferCore, ScopeCloseDoesNotConsumeLeaseFailure) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    int  drops {}, releases {};
    auto run = [&]() -> coro<empty> {
        auto engine = lf::Engine::make(runtime.handle()).unwrap();
        auto lease  = engine.resources()
                          .attach(empty {},
                                  Arc<rstd::dyn<lf::ManagedResource>>::make(
                                      OwnedCloseFailure { &drops, &releases }))
                          .unwrap();
        EXPECT_TRUE(lease.take_release_result().is_none());
        auto report = co_await engine.close();
        EXPECT_EQ(report.total_errors, usize(1));
        EXPECT_TRUE(engine.take_close_errors().is_empty());
        EXPECT_TRUE((co_await lease.wait_released()).is_err());
        auto result = lease.take_release_result();
        EXPECT_TRUE(result.is_some());
        EXPECT_TRUE(result->is_err());
        EXPECT_TRUE(lease.take_release_result().is_none());
        EXPECT_EQ(rstd::format("{}", result->as_ref().unwrap_err()), "connect cause 52"_str);
        result = None();
        EXPECT_EQ(drops, 1);
        EXPECT_TRUE((co_await lease.release()).unwrap_err().is_Contract());
        EXPECT_EQ((co_await engine.close()).total_errors, usize(1));
        EXPECT_TRUE(engine.take_close_errors().is_empty());
        EXPECT_EQ(releases, 1);
        co_return empty {};
    };
    runtime.block_on(run());
}

TEST(TransferCore, LeaseFailureCanTransferAfterScopeAndParentClose) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    int  drops {}, releases {};
    auto run = [&]() -> coro<empty> {
        auto                             engine = lf::Engine::make(runtime.handle()).unwrap();
        Option<lf::ResourceLease<empty>> lease;
        {
            auto scope = engine.scope().unwrap();
            lease      = Some(scope.resources()
                                  .attach(empty {},
                                          Arc<rstd::dyn<lf::ManagedResource>>::make(
                                              OwnedCloseFailure { &drops, &releases }))
                                  .unwrap());
            EXPECT_EQ((co_await engine.close()).total_errors, usize(1));
            EXPECT_TRUE(scope.take_close_errors().is_empty());
            EXPECT_TRUE(engine.take_close_errors().is_empty());
        }
        EXPECT_EQ(drops, 0);
        lease       = None();
        auto errors = engine.take_close_errors();
        EXPECT_EQ(errors.len(), usize(1));
        EXPECT_EQ(rstd::format("{}", errors[usize()]), "connect cause 52"_str);
        EXPECT_TRUE(engine.take_close_errors().is_empty());
        EXPECT_EQ((co_await engine.close()).total_errors, usize(1));
        errors.clear();
        EXPECT_EQ(drops, 1);
        EXPECT_EQ(releases, 1);
        co_return empty {};
    };
    runtime.block_on(run());
}

TEST(TransferCore, IntoScopeTransfersFutureCleanupFailure) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    int  drops {}, releases {};
    auto run = [&]() -> coro<empty> {
        auto engine = lf::Engine::make(runtime.handle()).unwrap();
        auto lease  = engine.resources()
                          .attach(usize(7),
                                  Arc<rstd::dyn<lf::ManagedResource>>::make(
                                      OwnedCloseFailure { &drops, &releases }))
                          .unwrap();
        EXPECT_EQ(rstd::move(lease).into_scope(), usize(7));
        EXPECT_EQ((co_await engine.close()).total_errors, usize(1));
        auto errors = engine.take_close_errors();
        EXPECT_EQ(errors.len(), usize(1));
        EXPECT_TRUE(engine.take_close_errors().is_empty());
        errors.clear();
        EXPECT_EQ(drops, 1);
        EXPECT_EQ(releases, 1);
        co_return empty {};
    };
    runtime.block_on(run());
}

TEST(TransferCore, ReleaseWaitCannotConsumeFailureAfterIntoScope) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    int  drops {}, releases {};
    auto run = [&]() -> coro<empty> {
        auto engine      = lf::Engine::make(runtime.handle()).unwrap();
        auto lease       = engine.resources()
                               .attach(empty {},
                                       Arc<rstd::dyn<lf::ManagedResource>>::make(
                                           OwnedCloseFailure { &drops, &releases }))
                               .unwrap();
        auto waiting     = lease.release().into_future();
        auto interrupted = co_await rstd::async::select(
            rstd::future::by_ref(waiting), rstd::async::sleep(Duration::from_millis(u64(1))));
        EXPECT_TRUE(interrupted.is_right());
        (void)rstd::move(lease).into_scope();
        EXPECT_TRUE((co_await rstd::move(waiting)).unwrap_err().is_Contract());
        EXPECT_EQ((co_await engine.close()).total_errors, usize(1));
        auto errors = engine.take_close_errors();
        EXPECT_EQ(errors.len(), usize(1));
        EXPECT_EQ(rstd::format("{}", errors[usize()]), "connect cause 52"_str);
        EXPECT_TRUE(engine.take_close_errors().is_empty());
        errors.clear();
        EXPECT_EQ(drops, 1);
        EXPECT_EQ(releases, 1);
        co_return empty {};
    };
    runtime.block_on(run());
}

TEST(TransferCore, CleanupTransferPreservesAllErrorsWithoutRecountingAncestors) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        auto           engine = lf::Engine::make(runtime.handle()).unwrap();
        auto           first  = engine.scope().unwrap();
        auto           second = engine.scope().unwrap();
        Vec<lf::Error> failures;
        failures.push(lf::Error::BodySource("one"_Str));
        failures.push(lf::Error::BodySource("two"_Str));
        first.record_cleanup_error(lf::collect_errors(rstd::move(failures)).unwrap_err());
        EXPECT_EQ((co_await first.close()).total_errors, usize(2));
        auto errors = first.take_close_errors();
        EXPECT_EQ(errors.len(), usize(2));
        second.record_cleanup_error(lf::collect_errors(rstd::move(errors)).unwrap_err());
        EXPECT_EQ((co_await second.close()).total_errors, usize(2));
        EXPECT_EQ((co_await engine.close()).total_errors, usize(2));
        EXPECT_TRUE(first.take_close_errors().is_empty());
        EXPECT_TRUE(engine.take_close_errors().is_empty());
        auto owned = second.take_close_errors();
        EXPECT_EQ(owned.len(), usize(2));
        EXPECT_EQ(rstd::format("{}", owned[usize()]), "one"_str);
        EXPECT_EQ(rstd::format("{}", owned[usize(1)]), "two"_str);
        co_return empty {};
    };
    runtime.block_on(run());
}

TEST(TransferCore, NestedScopesTransferAllErrorsBeyondDiagnosticLimit) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    int  drops {};
    auto run = [&]() -> coro<empty> {
        auto                      engine = lf::Engine::make(runtime.handle()).unwrap();
        Option<lf::TransferScope> inner;
        {
            auto outer = engine.scope().unwrap();
            inner      = Some(outer.handle().scope().unwrap());
            for (int i = 0; i < 20; ++i)
                (void)inner->resources().spawn(FailingTask { &drops }).unwrap();
            auto report = co_await engine.close();
            EXPECT_EQ(report.total_errors, usize(20));
            EXPECT_EQ(report.errors.len(), usize(16));
            EXPECT_TRUE(outer.take_close_errors().is_empty());
            EXPECT_TRUE(engine.take_close_errors().is_empty());
        }
        EXPECT_EQ(drops, 0);
        inner       = None();
        auto errors = engine.take_close_errors();
        EXPECT_EQ(errors.len(), usize(20));
        EXPECT_TRUE(engine.take_close_errors().is_empty());
        errors.clear();
        EXPECT_EQ(drops, 20);
        EXPECT_EQ((co_await engine.close()).total_errors, usize(20));
        co_return empty {};
    };
    runtime.block_on(run());
}

struct MultipleCleanupFailures : OwnedCloseFailure {
    auto run(lf::TransferContext context, lf::OutputSender<empty> output)
        -> coro<lf::ProtocolCompletion> {
        for (int i = 0; i < 3; ++i) {
            auto lease = context.resources()
                             .attach(empty {},
                                     Arc<rstd::dyn<lf::ManagedResource>>::make(
                                         OwnedCloseFailure { drops, releases }))
                             .unwrap();
            (void)rstd::move(lease).into_scope();
        }
        co_return lf::ProtocolCompletion { output.publish(empty {}), lf::RemoteOutcome::Confirmed };
    }
};

TEST(TransferCore, TransferRetainsEveryCleanupErrorWithoutParentDuplication) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    int  drops {}, releases {};
    auto run = [&]() -> coro<empty> {
        auto engine   = lf::Engine::make(runtime.handle()).unwrap();
        auto transfer = engine
                            .submit(lf::PreparedOperation<empty>::make(MultipleCleanupFailures {
                                OwnedCloseFailure { &drops, &releases } }))
                            .unwrap();
        EXPECT_TRUE((co_await transfer.output()).is_ok());
        auto report = co_await transfer.control().wait();
        EXPECT_EQ(report->cleanup.total_errors, usize(4));
        EXPECT_EQ((co_await engine.close()).total_errors, usize(4));
        EXPECT_TRUE(engine.take_close_errors().is_empty());
        auto errors = transfer.take_cleanup_errors();
        EXPECT_EQ(errors.len(), usize(4));
        EXPECT_TRUE(transfer.take_cleanup_errors().is_empty());
        errors.clear();
        EXPECT_EQ(drops, 4);
        EXPECT_EQ(releases, 4);
        EXPECT_EQ((co_await transfer.control().wait())->cleanup.total_errors, usize(4));
        co_return empty {};
    };
    runtime.block_on(run());
}

TEST(TransferCore, DiagnosticReportDoesNotRetainOutputError) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    int  drops {};
    auto task = [&]() -> coro<empty> {
        auto engine = lf::Engine::make(runtime.handle()).unwrap();
        auto transfer =
            engine.submit(lf::PreparedOperation<empty>::make(DiagnosticFailure { &drops, false }))
                .unwrap();
        auto report = co_await transfer.control().wait();
        EXPECT_TRUE(transfer.take_error().is_none());
        auto result = co_await transfer.output();
        EXPECT_TRUE(result.is_err());
        auto diagnostic = report->logical.result.as_ref().unwrap_err().clone();
        EXPECT_EQ(diagnostic.message.as_str(), "connect cause 73"_str);
        EXPECT_EQ(diagnostic.debug.as_str(), "connect cause 73"_str);
        EXPECT_EQ(report->cleanup.total_errors, usize(1));
        EXPECT_EQ(report->cleanup.errors[usize()].message.as_str(), "cleanup failed"_str);
        auto source =
            rstd::dyn<rstd::error::Error>::from_ref(result.as_ref().unwrap_err())->source();
        auto native = rstd::error::downcast_ref<rstd::io::Error>(source.unwrap()).unwrap();
        EXPECT_TRUE(
            rstd::error::downcast_ref<OwnedConnectCause>(native->get_ref().unwrap()).is_some());
        result = Err(lf::Error::Closed());
        EXPECT_EQ(drops, 1);
        EXPECT_EQ(diagnostic.message.as_str(), "connect cause 73"_str);
        EXPECT_TRUE((co_await transfer.output()).unwrap_err().is_OutputTaken());
        EXPECT_TRUE(transfer.take_error().is_none());
        EXPECT_TRUE((co_await transfer.control().wait())->logical.result.is_err());
        EXPECT_TRUE((co_await engine.close()).is_err());
        co_return empty {};
    };
    runtime.block_on(task());
    EXPECT_EQ(drops, 1);
}

TEST(TransferCore, PublishedOutputKeepsLateErrorForTransferOwner) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    int  drops {};
    auto task = [&]() -> coro<empty> {
        auto engine = lf::Engine::make(runtime.handle()).unwrap();
        auto transfer =
            engine.submit(lf::PreparedOperation<empty>::make(DiagnosticFailure { &drops, true }))
                .unwrap();
        auto report = co_await transfer.control().wait();
        EXPECT_TRUE(transfer.take_error().is_none());
        EXPECT_TRUE((co_await transfer.output()).is_ok());
        auto original = transfer.take_error();
        EXPECT_TRUE(original.is_some());
        EXPECT_EQ(rstd::format("{}", *original), "connect cause 73"_str);
        EXPECT_TRUE(transfer.take_error().is_none());
        original = None();
        EXPECT_EQ(drops, 1);
        EXPECT_EQ(report->logical.result.as_ref().unwrap_err().message.as_str(),
                  "connect cause 73"_str);
        EXPECT_TRUE((co_await engine.close()).is_err());
        co_return empty {};
    };
    runtime.block_on(task());
}

TEST(TransferCore, TypedOutputPrecedesProtocolConfirmationAndCleanup) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        auto engine = lf::Engine::make(runtime.handle()).unwrap();
        auto stats  = Stats::make(Counts {});
        auto transfer =
            engine.submit(lf::PreparedOperation<String>::make(Command { stats.clone() })).unwrap();
        auto result = co_await transfer.output();
        EXPECT_TRUE(result.is_ok());
        EXPECT_EQ(result->as_str(), "accepted locally"_str);
        EXPECT_EQ(stats->lock().unwrap()->commits, usize());
        EXPECT_TRUE((co_await transfer.output()).unwrap_err().is_OutputTaken());
        auto report = co_await transfer.control().wait();
        EXPECT_TRUE(report->logical.result.is_ok());
        EXPECT_EQ(report->logical.remote, lf::RemoteOutcome::Confirmed);
        EXPECT_EQ(stats->lock().unwrap()->commits, usize(1));
        EXPECT_EQ(stats->lock().unwrap()->closed, usize(1));
        EXPECT_TRUE((co_await transfer.control().wait())->cleanup.is_ok());
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(task());
}
TEST(TransferCore, FinalRejectionIsKnownAndNotCleanupFailure) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        auto    engine = lf::Engine::make(runtime.handle()).unwrap();
        auto    stats  = Stats::make(Counts {});
        Command command { stats.clone() };
        command.reject = true;
        auto transfer =
            engine.submit(lf::PreparedOperation<String>::make(rstd::move(command))).unwrap();
        EXPECT_TRUE((co_await transfer.output()).is_ok());
        auto report = co_await transfer.control().wait();
        EXPECT_TRUE(report->logical.result.is_err());
        EXPECT_EQ(report->logical.remote, lf::RemoteOutcome::Confirmed);
        EXPECT_TRUE(report->cleanup.is_ok());
        EXPECT_EQ(stats->lock().unwrap()->commits, usize());
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(task());
}
TEST(TransferCore, CancellationPreservesUnknownRemoteOutcomeAndNeverCommits) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        auto    engine = lf::Engine::make(runtime.handle()).unwrap();
        auto    stats  = Stats::make(Counts {});
        Command command { stats.clone() };
        command.work = Duration::from_secs(u64(10));
        auto transfer =
            engine.submit(lf::PreparedOperation<String>::make(rstd::move(command))).unwrap();
        (void)co_await transfer.output();
        auto control = transfer.control();
        control.cancel();
        auto report = co_await control.wait();
        EXPECT_TRUE(report->logical.result.is_err());
        EXPECT_EQ(report->logical.remote, lf::RemoteOutcome::Unknown);
        EXPECT_EQ(report->progress.accepted_bytes, Some(u64(3)));
        EXPECT_EQ(stats->lock().unwrap()->commits, usize());
        EXPECT_EQ(stats->lock().unwrap()->closed, usize(1));
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(task());
}
TEST(TransferCore, DroppingOwnerCancelsEvenWhenOutputAndControlSurvive) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        auto                        engine = lf::Engine::make(runtime.handle()).unwrap();
        auto                        stats  = Stats::make(Counts {});
        Option<lf::TransferControl> control;
        {
            Command command { stats.clone() };
            command.work = Duration::from_secs(u64(10));
            auto transfer =
                engine.submit(lf::PreparedOperation<String>::make(rstd::move(command))).unwrap();
            auto value = co_await transfer.output();
            control    = Some(transfer.control());
            EXPECT_TRUE(value.is_ok());
        }
        auto report = co_await control->wait();
        EXPECT_TRUE(report->stop_requested.is_some());
        EXPECT_EQ(stats->lock().unwrap()->commits, usize());
        EXPECT_EQ(stats->lock().unwrap()->closed, usize(1));
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(task());
}
TEST(TransferCore, MultipleResourcesReleaseAfterPartialSetupFailure) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        auto engine = lf::Engine::make(runtime.handle()).unwrap();
        auto a = Stats::make(Counts {}), b = Stats::make(Counts {});
        auto transfer =
            engine.submit(lf::PreparedOperation<empty>::make(TwoChannels { a.clone(), b.clone() }))
                .unwrap();
        EXPECT_TRUE((co_await transfer.output()).is_err());
        auto report = co_await transfer.control().wait();
        EXPECT_TRUE(report->logical.result.is_err());
        EXPECT_EQ(a->lock().unwrap()->closed, usize(1));
        EXPECT_EQ(b->lock().unwrap()->closed, usize(1));
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(task());
}
TEST(TransferCore, CanceledReleaseWaitKeepsActualCleanupAndCanBeWaitedAgain) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        auto engine = lf::Engine::make(runtime.handle()).unwrap();
        auto stats  = Stats::make(Counts {});
        auto lease =
            engine.resources()
                .attach(empty {},
                        Arc<rstd::dyn<lf::ManagedResource>>::make(Channel { stats.clone() }))
                .unwrap();
        auto first = co_await rstd::async::select(
            lease.release().into_future(), rstd::async::sleep(Duration::from_millis(u64(1))));
        EXPECT_TRUE(first.is_right());
        EXPECT_EQ(stats->lock().unwrap()->closed, usize());
        EXPECT_TRUE((co_await lease.release()).is_ok());
        EXPECT_EQ(stats->lock().unwrap()->releases, usize(1));
        EXPECT_EQ(stats->lock().unwrap()->closed, usize(1));
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(task());
}
TEST(TransferCore, RejectedSubmissionReturnsUnstartedInputForExplicitClose) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        auto engine = lf::Engine::make(runtime.handle()).unwrap();
        auto stats  = Stats::make(Counts {});
        EXPECT_TRUE((co_await engine.close()).is_ok());
        auto rejected =
            engine.submit(lf::PreparedOperation<String>::make(Command { stats.clone() }));
        EXPECT_TRUE(rejected.is_err());
        auto value = rstd::move(rejected).unwrap_err();
        EXPECT_TRUE(value.error.is_Closed());
        EXPECT_EQ(stats->lock().unwrap()->runs, usize());
        EXPECT_TRUE((co_await value.input.close()).is_ok());
        EXPECT_EQ(stats->lock().unwrap()->closed, usize(1));
        co_return empty {};
    };
    runtime.block_on(task());
}
TEST(TransferCore, ChildAttemptsShareParentBudget) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        auto engine   = lf::Engine::make(runtime.handle()).unwrap();
        auto transfer = engine
                            .submit(lf::PreparedOperation<usize>::make(ParentAttempt {}),
                                    { .max_attempts = usize(2) })
                            .unwrap();
        auto value    = co_await transfer.output();
        EXPECT_TRUE(value.is_ok());
        EXPECT_EQ(*value, usize(2));
        EXPECT_TRUE((co_await transfer.control().wait())->logical.result.is_ok());
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(task());
}
TEST(TransferCore, ByteQueueBoundsBackpressureAndReleasesQuota) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        auto engine = lf::Engine::make(runtime.handle()).unwrap();
        auto queue  = (co_await lf::ByteQueue::make(engine.resources(), usize(4))).unwrap();
        EXPECT_TRUE(queue.try_send(lf::Bytes::copy_from_slice("1234"_str.as_bytes())).is_ok());
        EXPECT_TRUE(queue.try_send(lf::Bytes::copy_from_slice("x"_str.as_bytes())).is_err());
        auto writing = queue.send(lf::Bytes::copy_from_slice("x"_str.as_bytes())).into_future();
        auto blocked = co_await rstd::async::select(
            rstd::future::by_ref(writing), rstd::async::sleep(Duration::from_millis(u64(1))));
        EXPECT_TRUE(blocked.is_right());
        auto bytes = co_await queue.next(usize(2));
        EXPECT_EQ((**bytes).as_slice(), "12"_str.as_bytes());
        EXPECT_TRUE((co_await rstd::move(writing)).is_ok());
        EXPECT_EQ(engine.resources().usage().bytes, usize(4));
        queue.release();
        EXPECT_EQ(engine.resources().usage().bytes, usize());
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(task());
}
TEST(TransferCore, ByteQueueTryReadPreservesPendingEofAndSingleReader) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        auto engine = lf::Engine::make(runtime.handle()).unwrap();
        auto queue  = (co_await lf::ByteQueue::make(engine.resources(), usize(4))).unwrap();
        EXPECT_TRUE(queue.try_next().unwrap().is_none());
        EXPECT_TRUE(queue.try_next(usize()).unwrap_err().is_InvalidOptions());
        EXPECT_TRUE(queue.try_send(lf::Bytes::copy_from_slice("1234"_str.as_bytes())).is_ok());
        EXPECT_EQ((**queue.try_next(usize(2)).unwrap()).as_slice(), "12"_str.as_bytes());
        EXPECT_EQ(queue.available(), usize(2));
        queue.finish();
        EXPECT_EQ((**queue.try_next().unwrap()).as_slice(), "34"_str.as_bytes());
        EXPECT_TRUE(queue.try_next().unwrap()->is_none());
        auto pending = (co_await lf::ByteQueue::make(engine.resources(), usize(4))).unwrap();
        {
            auto read   = pending.next().into_future();
            auto polled = co_await rstd::async::select(
                rstd::future::by_ref(read), rstd::async::sleep(Duration::from_millis(u64(1))));
            EXPECT_TRUE(polled.is_right());
            EXPECT_TRUE(pending.try_next().unwrap_err().is_Busy());
        }
        EXPECT_TRUE(pending.try_next().unwrap_err().is_Canceled());
        pending.release();
        queue.release();
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(task());
}

namespace
{
struct DelayedCommand : Command {
    auto run(lf::TransferContext context, lf::OutputSender<String> output)
        -> coro<lf::ProtocolCompletion> {
        auto ready = co_await context.delay(Duration::from_millis(u64(15)));
        if (ready.is_err())
            co_return lf::ProtocolCompletion { Err(rstd::move(ready).unwrap_err()),
                                               lf::RemoteOutcome::NotDispatched };
        co_return co_await Command::run(context.clone(), rstd::move(output));
    }
};
struct TokenTask {
    Stats stats;
    auto  run(lf::Cancellation canceled) -> coro<lf::Result<empty>> {
        (void)co_await canceled.stopped();
        co_await rstd::async::sleep(Duration::from_millis(u64(15)));
        {
            auto f = stats->lock().unwrap();
            ++f->closed;
        }
        co_return Ok(empty {});
    }
};
struct TaskCommand {
    Stats stats;
    auto  run(lf::TransferContext context, lf::OutputSender<empty> output)
        -> coro<lf::ProtocolCompletion> {
        auto task      = context.resources().spawn(TokenTask { stats.clone() }).unwrap();
        auto published = output.publish(empty {});
        auto done      = co_await context.delay(Duration::from_secs(u64(10)));
        co_return lf::ProtocolCompletion { rstd::move(done), lf::RemoteOutcome::Unknown };
    }
    void request_stop(lf::StopReason) {}
    auto release(lf::ReleaseContext) -> coro<lf::Result<empty>> { co_return Ok(empty {}); }
};
} // namespace
TEST(TransferCore, CanceledOutputAndReportWaitCanBeResumed) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        auto           engine = lf::Engine::make(runtime.handle()).unwrap();
        auto           stats  = Stats::make(Counts {});
        DelayedCommand command { Command { stats.clone() } };
        command.cleanup = Duration::from_millis(u64(20));
        auto transfer =
            engine.submit(lf::PreparedOperation<String>::make(rstd::move(command))).unwrap();
        auto early = co_await rstd::async::select(
            transfer.output().into_future(), rstd::async::sleep(Duration::from_millis(u64(1))));
        EXPECT_TRUE(early.is_right());
        EXPECT_TRUE((co_await transfer.output()).is_ok());
        auto waiting =
            co_await rstd::async::select(transfer.control().wait().into_future(),
                                         rstd::async::sleep(Duration::from_millis(u64(1))));
        EXPECT_TRUE(waiting.is_right());
        auto report = co_await transfer.control().wait();
        EXPECT_TRUE(report->logical.result.is_ok());
        EXPECT_EQ(stats->lock().unwrap()->releases, usize(1));
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(run());
}
TEST(TransferCore, PreparedCloseRetainsReleaseFutureAndRejectsResubmission) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        auto engine = lf::Engine::make(runtime.handle()).unwrap();
        auto stats  = Stats::make(Counts {});
        auto input  = lf::PreparedOperation<String>::make(Command { stats.clone() });
        auto early  = co_await rstd::async::select(
            input.close().into_future(), rstd::async::sleep(Duration::from_millis(u64(1))));
        EXPECT_TRUE(early.is_right());
        EXPECT_TRUE((co_await input.close()).is_ok());
        EXPECT_TRUE((co_await input.wait_closed()).is_ok());
        EXPECT_TRUE((co_await input.close()).unwrap_err().is_Contract());
        EXPECT_EQ(stats->lock().unwrap()->releases, usize(1));
        auto rejected = engine.submit(rstd::move(input));
        EXPECT_TRUE(rejected.is_err());
        auto value = rstd::move(rejected).unwrap_err();
        EXPECT_TRUE(value.error.is_Closed());
        EXPECT_TRUE((co_await value.input.wait_closed()).is_ok());
        EXPECT_EQ(stats->lock().unwrap()->runs, usize());
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(run());
}
TEST(TransferCore, QueuedCancellationSkipsDriverInvocation) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto engine  = lf::Engine::make(runtime.handle()).unwrap();
    auto stats   = Stats::make(Counts {});
    auto transfer =
        engine.submit(lf::PreparedOperation<String>::make(Command { stats.clone() })).unwrap();
    transfer.control().cancel();
    auto report = runtime.block_on(transfer.control().wait());
    EXPECT_TRUE(report->logical.result.is_err());
    EXPECT_EQ(stats->lock().unwrap()->runs, usize());
    EXPECT_EQ(stats->lock().unwrap()->closed, usize(1));
    EXPECT_TRUE(runtime.block_on(engine.close()).is_ok());
}
TEST(TransferCore, ParentCancellationJoinsManagedTaskCleanup) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        auto engine = lf::Engine::make(runtime.handle()).unwrap();
        auto stats  = Stats::make(Counts {});
        auto transfer =
            engine.submit(lf::PreparedOperation<empty>::make(TaskCommand { stats.clone() }))
                .unwrap();
        (void)co_await transfer.output();
        transfer.control().cancel();
        auto report = co_await transfer.control().wait();
        EXPECT_TRUE(report->logical.result.is_err());
        EXPECT_EQ(stats->lock().unwrap()->closed, usize(1));
        EXPECT_EQ(engine.resources().usage().tasks, usize());
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(run());
}
TEST(TransferCore, ClosingOneScopeDoesNotCancelItsSibling) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        auto    engine = lf::Engine::make(runtime.handle()).unwrap();
        auto    a = engine.scope().unwrap(), b = engine.scope().unwrap();
        auto    first = Stats::make(Counts {}), second = Stats::make(Counts {});
        Command ca { first.clone() }, cb { second.clone() };
        ca.work = cb.work = Duration::from_secs(u64(10));
        auto ta           = a.submit(lf::PreparedOperation<String>::make(rstd::move(ca))).unwrap();
        auto tb           = b.submit(lf::PreparedOperation<String>::make(rstd::move(cb))).unwrap();
        (void)co_await ta.output();
        (void)co_await tb.output();
        EXPECT_TRUE((co_await a.close()).is_ok());
        EXPECT_EQ(first->lock().unwrap()->closed, usize(1));
        EXPECT_EQ(second->lock().unwrap()->closed, usize());
        EXPECT_EQ(tb.control().snapshot().phase, lf::TransferPhase::Running);
        (void)co_await tb.control().close();
        EXPECT_TRUE((co_await b.close()).is_ok());
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(run());
}
namespace
{
struct SlowSource {
    Stats stats;
    auto  next() -> coro<lf::Result<Option<lf::Bytes>>> { co_return Ok(None()); }
    void  cancel() { stats->lock().unwrap()->stopped = true; }
    auto  close() -> coro<lf::Result<empty>> {
        ++stats->lock().unwrap()->releases;
        co_await rstd::async::sleep(Duration::from_millis(u64(12)));
        ++stats->lock().unwrap()->closed;
        co_return Ok(empty {});
    }
};
} // namespace
TEST(TransferCore, UploadCloseRetainsSourceReleaseAcrossCanceledWait) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        auto stats = Stats::make(Counts {});
        auto body  = lf::UploadBody::from_source(SlowSource { stats.clone() });
        body.cancel();
        auto first = co_await rstd::async::select(
            body.close().into_future(), rstd::async::sleep(Duration::from_millis(u64(1))));
        EXPECT_TRUE(first.is_right());
        EXPECT_TRUE((co_await body.close()).is_ok());
        EXPECT_TRUE((co_await body.wait_closed()).is_ok());
        EXPECT_EQ(stats->lock().unwrap()->releases, usize(1));
        EXPECT_EQ(stats->lock().unwrap()->closed, usize(1));
        co_return empty {};
    };
    runtime.block_on(run());
}
TEST(TransferCore, ExpiredHandleRejectsResourcesWithoutInvalidContext) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    Option<lf::EngineHandle> handle;
    {
        auto engine = lf::Engine::make(runtime.handle()).unwrap();
        handle      = Some(engine.handle());
        EXPECT_TRUE(runtime.block_on(engine.close()).is_ok());
    }
    EXPECT_TRUE(handle->resources().is_err());
    EXPECT_TRUE(handle->scope().is_err());
}
TEST(TransferCore, CapacityRejectsUnstartedInputAndAllowsResubmissionAfterRelease) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        lf::EngineOptions options;
        options.limits.operations = usize(1);
        auto    engine            = lf::Engine::make(runtime.handle(), options).unwrap();
        auto    first = Stats::make(Counts {}), second = Stats::make(Counts {});
        Command long_work { first.clone() };
        long_work.work = Duration::from_secs(u64(10));
        auto transfer =
            engine.submit(lf::PreparedOperation<String>::make(rstd::move(long_work))).unwrap();
        (void)co_await transfer.output();
        auto rejected =
            engine.submit(lf::PreparedOperation<String>::make(Command { second.clone() }));
        EXPECT_TRUE(rejected.is_err());
        auto input = rstd::move(rejected).unwrap_err();
        EXPECT_TRUE(input.error.is_Capacity());
        EXPECT_EQ(second->lock().unwrap()->runs, usize());
        (void)co_await transfer.control().close();
        EXPECT_EQ(engine.resources().usage().operations, usize());
        auto retry = engine.submit(rstd::move(input.input)).unwrap();
        EXPECT_TRUE((co_await retry.output()).is_ok());
        EXPECT_TRUE((co_await retry.control().wait())->logical.result.is_ok());
        EXPECT_EQ(second->lock().unwrap()->runs, usize(1));
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(run());
}
TEST(TransferCore, RejectedResourceCloseCanResumeAndRunsOnlyOnce) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        lf::EngineOptions options;
        options.limits.resources = usize(1);
        auto engine              = lf::Engine::make(runtime.handle(), options).unwrap();
        auto first = Stats::make(Counts {}), second = Stats::make(Counts {});
        auto lease =
            engine.resources()
                .attach(usize(1),
                        Arc<rstd::dyn<lf::ManagedResource>>::make(Channel { first.clone() }))
                .unwrap();
        auto rejected = engine.resources().attach(
            usize(2), Arc<rstd::dyn<lf::ManagedResource>>::make(Channel { second.clone() }));
        EXPECT_TRUE(rejected.is_err());
        auto value = rstd::move(rejected).unwrap_err();
        EXPECT_TRUE(value.error.is_Capacity());
        EXPECT_EQ(value.input.access, usize(2));
        auto interrupted = co_await rstd::async::select(
            value.input.close().into_future(), rstd::async::sleep(Duration::from_millis(u64(1))));
        EXPECT_TRUE(interrupted.is_right());
        EXPECT_TRUE((co_await value.input.close()).is_ok());
        EXPECT_TRUE((co_await value.input.wait_closed()).is_ok());
        EXPECT_TRUE((co_await value.input.close()).unwrap_err().is_Contract());
        EXPECT_EQ(second->lock().unwrap()->releases, usize(1));
        EXPECT_EQ(second->lock().unwrap()->closed, usize(1));
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(run());
}
TEST(TransferCore, ByteQueueObservationDoesNotConsumeOrCancelTheReader) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto task    = [&]() -> coro<empty> {
        auto engine = lf::Engine::make(runtime.handle()).unwrap();
        auto queue  = (co_await lf::ByteQueue::make(engine.resources(), usize(2))).unwrap();
        EXPECT_FALSE(queue.readable().unwrap());
        EXPECT_TRUE(
            (co_await rstd::async::timeout(queue.wait_readable().into_future(), Duration {}))
                .is_err());
        EXPECT_FALSE(queue.readable().unwrap());
        auto changed = queue.changed();
        queue.try_send(lf::Bytes::copy_from_slice("ab"_str.as_bytes())).unwrap();
        co_await rstd::move(changed);
        EXPECT_TRUE((co_await queue.wait_readable()).is_ok());
        EXPECT_TRUE(
            (co_await rstd::async::timeout(queue.wait_writable().into_future(), Duration {}))
                .is_err());
        EXPECT_TRUE(queue.readable().unwrap());
        auto readable = queue.try_next().unwrap();
        EXPECT_EQ((**readable).as_slice(), "ab"_str.as_bytes());
        EXPECT_TRUE((co_await queue.wait_writable()).is_ok());
        queue.finish();
        EXPECT_TRUE((co_await queue.wait_readable()).is_ok());
        EXPECT_TRUE(queue.try_next().unwrap()->is_none());
        EXPECT_TRUE((co_await queue.wait_writable()).unwrap_err().is_Closed());
        queue.release();
        auto failed = (co_await lf::ByteQueue::make(engine.resources(), usize(2))).unwrap();
        failed.fail(lf::Error::TooLarge());
        EXPECT_TRUE((co_await failed.wait_readable()).is_ok());
        EXPECT_TRUE((co_await failed.wait_writable()).unwrap_err().is_Closed());
        EXPECT_TRUE(failed.try_next().unwrap_err().is_TooLarge());
        EXPECT_TRUE(failed.try_next().unwrap_err().is_Closed());
        failed.release();
        auto canceled = (co_await lf::ByteQueue::make(engine.resources(), usize(2))).unwrap();
        engine.request_close();
        EXPECT_TRUE((co_await canceled.wait_readable()).unwrap_err().is_Canceled());
        canceled.release();
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(task());
}

TEST(TransferCore, ByteQueueFailureBelongsToReader) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    int  drops {};
    auto task = [&]() -> coro<empty> {
        auto engine = lf::Engine::make(runtime.handle()).unwrap();
        auto queue  = (co_await lf::ByteQueue::make(engine.resources(), usize(8))).unwrap();
        {
            auto pending = queue.next().into_future();
            auto waiting = co_await rstd::async::select(
                rstd::future::by_ref(pending), rstd::async::sleep(Duration::from_millis(u64(1))));
            EXPECT_TRUE(waiting.is_right());
            queue.fail(lf::Error::Connect(rstd::io::Error::with_source(
                rstd::io::ErrorKind { rstd::io::ErrorKind::TimedOut },
                Box<rstd::dyn<rstd::error::Error>>::make(OwnedConnectCause(drops, 91)))));
        }
        EXPECT_TRUE(queue.readable().unwrap());
        EXPECT_TRUE((co_await queue.wait_readable()).is_ok());
        EXPECT_TRUE((co_await queue.wait_readable()).is_ok());
        EXPECT_TRUE((co_await queue.wait_writable()).unwrap_err().is_Closed());
        EXPECT_TRUE(queue.try_send({}).unwrap_err().is_Closed());
        EXPECT_EQ(drops, 0);
        auto result = co_await queue.next();
        EXPECT_TRUE(result.is_err());
        auto source =
            rstd::dyn<rstd::error::Error>::from_ref(result.as_ref().unwrap_err())->source();
        auto native  = rstd::error::downcast_ref<rstd::io::Error>(source.unwrap()).unwrap();
        auto payload = rstd::error::downcast_ref<OwnedConnectCause>(native->get_ref().unwrap());
        EXPECT_EQ(payload.unwrap()->value, 91);
        EXPECT_TRUE(queue.try_next().unwrap_err().is_Closed());
        queue.fail(lf::Error::TooLarge());
        EXPECT_TRUE((co_await queue.next()).unwrap_err().is_Closed());
        queue.finish();
        EXPECT_TRUE(queue.try_next().unwrap_err().is_Closed());
        result = Err(lf::Error::Closed());
        EXPECT_EQ(drops, 1);
        queue.release();
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(task());
    EXPECT_EQ(drops, 1);
}
