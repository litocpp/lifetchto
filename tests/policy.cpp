#include <rstd/test/gtest.hpp>
#include <stdlib.h>
import lifetchto.http;
using namespace rstd::prelude;
using namespace rstd::literals;
namespace lf = lifetchto;
namespace h  = lifetchto::http;
using lf::Arc;
using lf::Bytes;
using lf::coro;
using lf::Duration;
namespace
{
auto request(ref<str> path = "http://example.test/item"_str, ref<str> method = "GET"_str)
    -> h::ExecutionRequest {
    return {
        { lihttpto::Url::parse_http(path).unwrap(), lihttpto::Method::parse(method).unwrap(), {} },
        {}
    };
}
struct Reply {
    u16               status { 200 };
    lihttpto::Headers headers;
    Bytes             data { Bytes::copy_from_slice("payload"_str.as_bytes()) };
    bool              connect_error {}, source_error {};
    lf::RemoteOutcome remote { lf::RemoteOutcome::Confirmed };
};
struct Seen {
    lihttpto::RequestParts    parts;
    Bytes                     body;
    Option<lihttpto::Headers> trailers;
};
struct Scenario {
    Vec<Reply> replies;
    Vec<Seen>  seen;
    usize      prepared {}, selected {}, closes {};
    bool       closed {};
};
using Script = Arc<rstd::sync::Mutex<Scenario>>;
struct Cursor {
    bool sent {}, canceled {}, closed {};
};
struct Source {
    Arc<rstd::sync::Mutex<Cursor>> cursor { Arc<rstd::sync::Mutex<Cursor>>::make(Cursor {}) };
    Bytes                          bytes;
    bool                           fail {};
    auto                           next() const -> coro<lf::Result<Option<Bytes>>> {
        auto f = cursor->lock().unwrap();
        if (f->canceled) co_return Err(lf::Error::Canceled());
        if (! f->sent) {
            f->sent = true;
            co_return Ok(Some(bytes.clone()));
        }
        if (fail) co_return Err(lf::Error::BodySource(String::make("truncated source"_str)));
        co_return Ok(None());
    }
    auto trailers() const -> lf::Result<Option<lihttpto::Headers>> {
        return Ok(Some(lihttpto::Headers {}));
    }
    void cancel() const { cursor->lock().unwrap()->canceled = true; }
    auto close() const -> coro<lf::Result<empty>> {
        cursor->lock().unwrap()->closed = true;
        co_return Ok(empty {});
    }
};
struct ExchangeState {
    rstd::sync::Mutex<Option<h::BodyControl>> response { None<h::BodyControl>() };
};
struct Exchange {
    h::ExchangeInput   input;
    Script             script;
    Arc<ExchangeState> state { Arc<ExchangeState>::make() };
    auto               run(lf::TransferContext context, lf::OutputSender<h::ResponseMessage> output)
        -> coro<lf::ProtocolCompletion>;
    void request_stop(lf::StopReason reason) {
        input.body.cancel();
        auto f = state->response.lock().unwrap();
        if (f->is_some()) (*f)->cancel(reason);
    }
    auto release(lf::ReleaseContext) -> coro<lf::Result<empty>> { return input.body.close(); }
};
struct Backend {
    Script             script;
    h::BackendIdentity id;
    auto prepare(lihttpto::RequestParts parts, h::UploadMetadata upload, h::RouteChoice route) const
        -> lf::Result<h::PreparedExchange> {
        {
            auto f = script->lock().unwrap();
            ++f->prepared;
            if (f->closed) return Err(lf::Error::Closed());
        }
        auto parsed = lihttpto::PreparedRequest::make(
            rstd::move(parts),
            upload.length,
            usize(65536),
            upload.trailers.is_some() ? rstd::addressof(*upload.trailers) : nullptr);
        if (parsed.is_err())
            return Err(lf::Error::InvalidOptions(String::make("invalid request"_str)));
        auto scope = h::CacheScope { id.clone(), route.clone() };
        return Ok(h::PreparedExchange::make(rstd::move(parsed).unwrap(),
                                            id.clone(),
                                            rstd::move(upload),
                                            rstd::move(route),
                                            rstd::move(scope)));
    }
    auto operation(h::ExchangeInput input) const
        -> rstd::Result<lf::PreparedOperation<h::ResponseMessage>, lf::Rejected<h::ExchangeInput>> {
        if (! id.matches(input.prepared.backend_identity()))
            return Err(lf::Rejected<h::ExchangeInput> {
                lf::Error::Contract(String::make("foreign preparation"_str)), rstd::move(input) });
        return Ok(lf::PreparedOperation<h::ResponseMessage>::make(
            Exchange { rstd::move(input), script.clone() }));
    }
    void request_close() const { script->lock().unwrap()->closed = true; }
    auto close() const -> coro<lf::Result<empty>> {
        auto f    = script->lock().unwrap();
        f->closed = true;
        ++f->closes;
        co_return Ok(empty {});
    }
};
struct Routes {
    Script script;
    auto   select(lihttpto::RequestParts, lf::TransferContext) const
        -> coro<lf::Result<h::RouteChoice>> {
        ++script->lock().unwrap()->selected;
        co_return Ok(h::RouteChoice {});
    }
    void request_close() const {}
    auto close() const -> coro<lf::Result<empty>> { co_return Ok(empty {}); }
};
struct Factory {
    Arc<rstd::sync::Mutex<usize>> opens;
    bool                          fail {};
    auto open(lf::TransferContext) const -> coro<lf::Result<lf::UploadBody>> {
        ++*opens->lock().unwrap();
        if (fail) co_return Err(lf::Error::BodySource(String::make("factory failed"_str)));
        co_return Ok(lf::UploadBody::bytes(Bytes::copy_from_slice("abc"_str.as_bytes())));
    }
    void request_stop(lf::StopReason) const {}
    auto release(lf::ReleaseContext) const -> coro<lf::Result<empty>> { co_return Ok(empty {}); }
};
struct ForwardStore {
    Arc<rstd::dyn<h::CacheStore>> inner;
    Arc<rstd::sync::Mutex<usize>> lookups;
    auto                          lookup(h::CacheKey key, lf::TransferContext context) const
        -> coro<lf::Result<h::CacheLookup>> {
        ++*lookups->lock().unwrap();
        return inner->lookup(rstd::move(key), rstd::move(context));
    }
    auto begin_write(h::CacheWriteSpec spec, lf::TransferContext context) const {
        return inner->begin_write(rstd::move(spec), rstd::move(context));
    }
    auto
    update(u64 id, u64 generation, h::CacheMetadata metadata, lf::TransferContext context) const {
        return inner->update(id, generation, rstd::move(metadata), rstd::move(context));
    }
    auto invalidate(h::CacheTarget target, lf::TransferContext context) const {
        return inner->invalidate(rstd::move(target), rstd::move(context));
    }
    void request_close() const { inner->request_close(); }
    auto close() const { return inner->close(); }
};
} // namespace
namespace rstd
{
template<>
struct Impl<h::ResponseSource, Source> : ImplBase<Source> {
    auto next() const { return this->self().next(); }
    auto trailers() const { return this->self().trailers(); }
    void cancel() const { this->self().cancel(); }
    auto close() const { return this->self().close(); }
};
template<>
struct Impl<h::ExchangeBackend, Backend> : ImplBase<Backend> {
    auto prepare(lihttpto::RequestParts p, h::UploadMetadata u, h::RouteChoice r) const {
        return this->self().prepare(rstd::move(p), rstd::move(u), rstd::move(r));
    }
    auto operation(h::ExchangeInput i) const { return this->self().operation(rstd::move(i)); }
    void request_close() const { this->self().request_close(); }
    auto close() const { return this->self().close(); }
};
template<>
struct Impl<h::RouteProvider, Routes> : ImplBase<Routes> {
    auto select(lihttpto::RequestParts p, lf::TransferContext c) const {
        return this->self().select(rstd::move(p), rstd::move(c));
    }
    void request_close() const { this->self().request_close(); }
    auto close() const { return this->self().close(); }
};
template<>
struct Impl<lf::UploadFactory, Factory> : ImplBase<Factory> {
    auto open(lf::TransferContext c) const { return this->self().open(rstd::move(c)); }
    void request_stop(lf::StopReason r) const { this->self().request_stop(r); }
    auto release(lf::ReleaseContext c) const { return this->self().release(c); }
};
template<>
struct Impl<h::CacheStore, ForwardStore> : ImplBase<ForwardStore> {
    auto lookup(h::CacheKey k, lf::TransferContext c) const {
        return this->self().lookup(rstd::move(k), rstd::move(c));
    }
    auto begin_write(h::CacheWriteSpec s, lf::TransferContext c) const {
        return this->self().begin_write(rstd::move(s), rstd::move(c));
    }
    auto update(u64 i, u64 g, h::CacheMetadata m, lf::TransferContext c) const {
        return this->self().update(i, g, rstd::move(m), rstd::move(c));
    }
    auto invalidate(h::CacheTarget t, lf::TransferContext c) const {
        return this->self().invalidate(rstd::move(t), rstd::move(c));
    }
    void request_close() const { this->self().request_close(); }
    auto close() const { return this->self().close(); }
};
} // namespace rstd
namespace
{
auto Exchange::run(lf::TransferContext context, lf::OutputSender<h::ResponseMessage> output)
    -> coro<lf::ProtocolCompletion> {
    auto counted = context.begin_attempt();
    if (counted.is_err())
        co_return lf::ProtocolCompletion { Err(rstd::move(counted).unwrap_err()),
                                           lf::RemoteOutcome::NotDispatched };
    Vec<u8> uploaded;
    for (;;) {
        auto bytes = co_await input.body.next(usize(32));
        if (bytes.is_err())
            co_return lf::ProtocolCompletion { Err(rstd::move(bytes).unwrap_err()),
                                               lf::RemoteOutcome::Unknown };
        if (bytes->is_none()) break;
        uploaded.extend_from_slice((**bytes).as_slice());
    }
    Reply reply;
    {
        auto f     = script->lock().unwrap();
        auto index = f->seen.len();
        f->seen.push({ input.prepared.parts().clone(),
                       Bytes::copy_from_slice(uploaded.as_slice()),
                       input.body.trailers().clone() });
        if (index < f->replies.len()) reply = rstd::move(f->replies[index]);
    }
    if (reply.connect_error)
        co_return lf::ProtocolCompletion {
            Err(lf::Error::Connect(rstd::io::Error::from_kind(
                rstd::io::ErrorKind { rstd::io::ErrorKind::ConnectionRefused }))),
            reply.remote
        };
    auto body    = h::ResponseBody::from_source(Arc<rstd::dyn<h::ResponseSource>>::make(
        Source { { Arc<rstd::sync::Mutex<Cursor>>::make(Cursor {}) },
                 rstd::move(reply.data),
                 reply.source_error }));
    auto control = body.completion();
    {
        auto f = state->response.lock().unwrap();
        *f     = Some(control.clone());
    }
    auto sent = output.publish(
        { { lihttpto::StatusCode::make(reply.status).unwrap(), rstd::move(reply.headers), {}, {} },
          rstd::move(body) });
    if (sent.is_err()) control.cancel();
    auto done = co_await control.wait();
    co_return lf::ProtocolCompletion { rstd::move(done), reply.remote };
}
auto finish(lf::Transfer<h::ResponseMessage>& transfer) -> coro<lf::Result<Bytes>> {
    auto response = co_await transfer.output();
    if (response.is_err()) co_return Err(rstd::move(response).unwrap_err());
    Vec<u8> bytes;
    for (;;) {
        auto part = co_await response->body.next();
        if (part.is_err()) co_return Err(rstd::move(part).unwrap_err());
        if (part->is_none()) break;
        bytes.extend_from_slice((**part).as_slice());
    }
    auto report = co_await transfer.control().wait();
    if (report->logical.result.is_err())
        co_return Err(report->logical.result.as_ref().unwrap_err().clone());
    co_return Ok(Bytes::copy_from_slice(bytes.as_slice()));
}
auto binding(Script script) -> h::BackendBinding {
    return h::BackendBinding::Shared(
        Arc<rstd::dyn<h::ExchangeBackend>>::make(Backend { rstd::move(script), {} }));
}
auto retry_policy() -> h::ExecutionPolicy {
    h::ExecutionPolicy policy;
    h::RetryPolicy     retry;
    retry.methods.push(String::make("GET"_str));
    retry.statuses.push(u16(503));
    retry.backoff = Duration::from_millis(u64(1));
    policy.retry  = Some(rstd::move(retry));
    return policy;
}
auto redirection(u16 status, ref<str> target) -> Reply {
    Reply reply;
    reply.status = status;
    reply.headers.add("Location"_str, target).unwrap();
    return reply;
}
} // namespace
TEST(HttpPolicy, RetryReusesPreparationAndRoutingAndReportsEachAttempt) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        auto engine = lf::Engine::make(runtime.handle()).unwrap();
        auto script = Script::make(Scenario {});
        {
            auto  f = script->lock().unwrap();
            Reply fail;
            fail.status = u16(503);
            f->replies.push(rstd::move(fail));
            f->replies.push(Reply {});
        }
        h::ClientConfig config;
        config.policy = retry_policy();
        auto client =
            h::Client::make(engine.handle(), binding(script.clone()), rstd::move(config)).unwrap();
        h::ExecutionObserver observer;
        h::ExecuteOptions    options;
        options.report = Some(observer.clone());
        options.routing.shared_provider =
            Some(Arc<rstd::dyn<h::RouteProvider>>::make(Routes { script.clone() }));
        auto transfer = client.execute(request(), rstd::move(options)).unwrap();
        EXPECT_TRUE((co_await finish(transfer)).is_ok());
        auto report = (co_await observer.wait()).unwrap();
        EXPECT_EQ(report.facts.attempts, usize(2));
        EXPECT_EQ(report.transfer->progress.attempts, usize(2));
        EXPECT_EQ(script->lock().unwrap()->prepared, usize(1));
        EXPECT_EQ(script->lock().unwrap()->selected, usize(1));
        EXPECT_TRUE((co_await client.close()).is_ok());
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(run());
}
TEST(HttpPolicy, RedirectsPreserveOrRewriteUploadAndFactoryReopens) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        for (auto status : array<u16, 5> { u16(301), u16(302), u16(303), u16(307), u16(308) }) {
            auto engine = lf::Engine::make(runtime.handle()).unwrap();
            auto script = Script::make(Scenario {});
            script->lock().unwrap()->replies.push(redirection(status, "/next"_str));
            h::ClientConfig config;
            config.policy.redirect = Some(h::RedirectPolicy {});
            auto client =
                h::Client::make(engine.handle(), binding(script.clone()), rstd::move(config))
                    .unwrap();
            auto              opens = Arc<rstd::sync::Mutex<usize>>::make(usize());
            auto              req   = request("http://example.test/start"_str, "POST"_str);
            lihttpto::Headers trailers;
            trailers.add("X-End"_str, "yes"_str).unwrap();
            req.body = h::UploadPlan::repeatable(
                { Some(u64(3)) },
                Arc<rstd::dyn<lf::UploadFactory>>::make(Factory { opens.clone() }),
                Some(rstd::move(trailers)));
            auto transfer = client.execute(rstd::move(req)).unwrap();
            EXPECT_TRUE((co_await finish(transfer)).is_ok());
            bool replay = status >= u16(307);
            {
                auto f = script->lock().unwrap();
                EXPECT_EQ(f->seen.len(), usize(2));
                EXPECT_EQ(f->seen[usize(1)].parts.method.as_ref(), replay ? "POST"_str : "GET"_str);
                EXPECT_EQ(f->seen[usize(1)].body.len(), replay ? usize(3) : usize());
                EXPECT_EQ(*opens->lock().unwrap(), replay ? usize(2) : usize(1));
                if (replay) EXPECT_TRUE(f->seen[usize(1)].trailers.is_some());
            }
            EXPECT_TRUE((co_await client.close()).is_ok());
            EXPECT_TRUE((co_await engine.close()).is_ok());
        }
        co_return empty {};
    };
    runtime.block_on(run());
}
TEST(HttpPolicy, OnceUploadRefusesReplayAndFactoryFailureNeverDispatches) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        auto engine = lf::Engine::make(runtime.handle()).unwrap();
        auto script = Script::make(Scenario {});
        script->lock().unwrap()->replies.push(redirection(u16(307), "/again"_str));
        h::ClientConfig config;
        config.policy.redirect = Some(h::RedirectPolicy {});
        auto client =
            h::Client::make(engine.handle(), binding(script.clone()), rstd::move(config)).unwrap();
        auto req = request();
        req.body = h::UploadPlan::once(
            lf::UploadBody::bytes(Bytes::copy_from_slice("abc"_str.as_bytes())));
        auto transfer = client.execute(rstd::move(req)).unwrap();
        auto result   = co_await finish(transfer);
        EXPECT_TRUE(result.is_err());
        EXPECT_TRUE(result.as_ref().unwrap_err().is_ReplayUnavailable());
        EXPECT_EQ(script->lock().unwrap()->seen.len(), usize(1));
        auto opens  = Arc<rstd::sync::Mutex<usize>>::make(usize());
        auto failed = request();
        failed.body = h::UploadPlan::repeatable(
            { Some(u64(3)) },
            Arc<rstd::dyn<lf::UploadFactory>>::make(Factory { opens.clone(), true }));
        auto other = client.execute(rstd::move(failed)).unwrap();
        EXPECT_TRUE((co_await finish(other)).is_err());
        EXPECT_EQ(script->lock().unwrap()->seen.len(), usize(1));
        EXPECT_TRUE((co_await client.close()).is_ok());
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(run());
}
TEST(HttpPolicy, CrossOriginStripsSensitiveFieldsAndScopedIdempotence) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        auto engine = lf::Engine::make(runtime.handle()).unwrap();
        auto script = Script::make(Scenario {});
        {
            auto f = script->lock().unwrap();
            f->replies.push(redirection(u16(307), "http://else.test/new"_str));
            Reply failed;
            failed.status = u16(503);
            f->replies.push(rstd::move(failed));
        }
        h::ClientConfig config;
        config.policy                            = retry_policy();
        config.policy.retry->business_idempotent = true;
        h::RedirectPolicy redirect;
        redirect.allow_cross_origin = true;
        redirect.remove_on_cross_origin.push(String::make("X-Secret"_str));
        config.policy.redirect = Some(rstd::move(redirect));
        auto client =
            h::Client::make(engine.handle(), binding(script.clone()), rstd::move(config)).unwrap();
        auto req = request("http://example.test/start"_str, "POST"_str);
        for (auto name : array<ref<str>, 4> {
                 "Authorization"_str, "Cookie"_str, "Proxy-Authorization"_str, "X-Secret"_str })
            req.parts.headers.add(name, "sensitive"_str).unwrap();
        req.parts.headers.add("X-Keep"_str, "first"_str).unwrap();
        req.parts.headers.add("X-Keep"_str, "second"_str).unwrap();
        auto transfer = client.execute(rstd::move(req)).unwrap();
        EXPECT_TRUE((co_await finish(transfer)).is_ok());
        {
            auto f = script->lock().unwrap();
            EXPECT_EQ(f->seen.len(), usize(2));
            auto& fields = f->seen[usize(1)].parts.headers;
            EXPECT_FALSE(fields.contains("authorization"_str));
            EXPECT_FALSE(fields.contains("cookie"_str));
            EXPECT_FALSE(fields.contains("proxy-authorization"_str));
            EXPECT_FALSE(fields.contains("x-secret"_str));
            EXPECT_EQ(fields.get_all("x-keep"_str).len(), usize(2));
        }
        EXPECT_TRUE((co_await client.close()).is_ok());
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(run());
}
TEST(HttpPolicy, DeadlineStopsRetryAfterAndAttemptBudgetCapsRetry) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        for (bool budget : array<bool, 2> { false, true }) {
            auto  engine = lf::Engine::make(runtime.handle()).unwrap();
            auto  script = Script::make(Scenario {});
            Reply fail;
            fail.status = u16(503);
            fail.headers.add("Retry-After"_str, "1"_str).unwrap();
            script->lock().unwrap()->replies.push(rstd::move(fail));
            h::ClientConfig config;
            config.policy = retry_policy();
            if (budget) config.policy.retry->respect_retry_after = false;
            config.total_timeout = Some(Duration::from_millis(u64(20)));
            auto client =
                h::Client::make(engine.handle(), binding(script.clone()), rstd::move(config))
                    .unwrap();
            h::ExecuteOptions options;
            if (budget) options.transfer.max_attempts = usize(1);
            auto transfer = client.execute(request(), rstd::move(options)).unwrap();
            auto result   = co_await finish(transfer);
            EXPECT_TRUE(result.is_err());
            EXPECT_TRUE(budget ? result.as_ref().unwrap_err().is_AttemptsExhausted()
                               : result.as_ref().unwrap_err().is_Timeout());
            EXPECT_EQ(script->lock().unwrap()->seen.len(), usize(1));
            EXPECT_TRUE((co_await client.close()).is_ok());
            EXPECT_TRUE((co_await engine.close()).is_ok());
        }
        co_return empty {};
    };
    runtime.block_on(run());
}
TEST(HttpPolicy, PartialDispatchIsNotRetriedWithoutBusinessPermission) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        auto  engine = lf::Engine::make(runtime.handle()).unwrap();
        auto  script = Script::make(Scenario {});
        Reply fail;
        fail.connect_error = true;
        fail.remote        = lf::RemoteOutcome::Unknown;
        script->lock().unwrap()->replies.push(rstd::move(fail));
        h::ClientConfig config;
        config.policy = retry_policy();
        auto client =
            h::Client::make(engine.handle(), binding(script.clone()), rstd::move(config)).unwrap();
        auto transfer =
            client.execute(request("http://example.test/item"_str, "POST"_str)).unwrap();
        EXPECT_TRUE((co_await finish(transfer)).is_err());
        EXPECT_EQ(script->lock().unwrap()->seen.len(), usize(1));
        EXPECT_EQ((co_await transfer.control().wait())->logical.remote, lf::RemoteOutcome::Unknown);
        EXPECT_TRUE((co_await client.close()).is_ok());
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(run());
}
TEST(HttpPolicy, SharedBindingsSurviveSiblingCloseAndOwnedBindingCloses) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        auto            engine  = lf::Engine::make(runtime.handle()).unwrap();
        auto            script  = Script::make(Scenario {});
        auto            backend = binding(script.clone()).backend();
        auto            cache   = h::MemoryCache::make(engine.resources()).unwrap();
        h::ClientConfig aconfig, bconfig;
        aconfig.cache = Some(h::StoreBinding::Shared(cache.store()));
        bconfig.cache = Some(h::StoreBinding::Shared(cache.store()));
        auto a        = h::Client::make(engine.handle(),
                                        h::BackendBinding::Shared(backend.clone()),
                                        rstd::move(aconfig))
                            .unwrap(),
             b        = h::Client::make(engine.handle(),
                                        h::BackendBinding::Shared(backend.clone()),
                                        rstd::move(bconfig))
                            .unwrap();
        EXPECT_TRUE((co_await a.close()).is_ok());
        EXPECT_EQ(script->lock().unwrap()->closes, usize());
        auto transfer = b.execute(request()).unwrap();
        EXPECT_TRUE((co_await finish(transfer)).is_ok());
        EXPECT_TRUE((co_await b.close()).is_ok());
        auto owner =
            h::Client::make(engine.handle(), h::BackendBinding::Owned(backend.clone())).unwrap();
        EXPECT_TRUE((co_await owner.close()).is_ok());
        EXPECT_EQ(script->lock().unwrap()->closes, usize(1));
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(run());
}
TEST(HttpCache, PublicStoreCachesAndPartitionsAndAbortsTruncatedBodies) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        auto engine = lf::Engine::make(runtime.handle()).unwrap();
        auto script = Script::make(Scenario {});
        {
            auto f = script->lock().unwrap();
            for (int i = 0; i < 5; ++i) {
                Reply reply;
                reply.headers.add("Cache-Control"_str, "max-age=60"_str).unwrap();
                reply.source_error = i == 2;
                f->replies.push(rstd::move(reply));
            }
        }
        auto cache   = h::MemoryCache::make(engine.resources()).unwrap();
        auto lookups = Arc<rstd::sync::Mutex<usize>>::make(usize());
        auto store =
            Arc<rstd::dyn<h::CacheStore>>::make(ForwardStore { cache.store(), lookups.clone() });
        h::ClientConfig config;
        config.policy.cache = h::CacheMode::Default;
        config.cache        = Some(h::StoreBinding::Shared(store.clone()));
        auto client =
            h::Client::make(engine.handle(), binding(script.clone()), rstd::move(config)).unwrap();
        for (int i = 0; i < 4; ++i) {
            h::ExecuteOptions options;
            options.partition = String::make(i < 2 ? "a"_str : "b"_str);
            h::ExecutionObserver report;
            options.report = Some(report.clone());
            auto transfer  = client.execute(request(), rstd::move(options)).unwrap();
            EXPECT_TRUE((co_await finish(transfer)).is_ok());
            EXPECT_EQ((co_await report.wait()).unwrap().facts.from_cache, i % 2 == 1);
        }
        EXPECT_EQ(script->lock().unwrap()->seen.len(), usize(2));
        auto broken = client.execute(request("http://example.test/broken"_str)).unwrap();
        EXPECT_TRUE((co_await finish(broken)).is_err());
        auto retry = client.execute(request("http://example.test/broken"_str)).unwrap();
        EXPECT_TRUE((co_await finish(retry)).is_ok());
        EXPECT_EQ(script->lock().unwrap()->seen.len(), usize(4));
        EXPECT_EQ(*lookups->lock().unwrap(), usize(6));
        h::ExecuteOptions  only;
        h::ExecutionPolicy policy;
        policy.cache = h::CacheMode::OnlyIfCached;
        only.policy  = Some(rstd::move(policy));
        auto miss =
            client.execute(request("http://example.test/missing"_str), rstd::move(only)).unwrap();
        auto result = co_await finish(miss);
        EXPECT_TRUE(result.as_ref().unwrap_err().is_CacheMiss());
        EXPECT_EQ(script->lock().unwrap()->seen.len(), usize(4));
        EXPECT_TRUE((co_await client.close()).is_ok());
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(run());
}
namespace
{
struct ContextDriver {
    auto run(lf::TransferContext context, lf::OutputSender<lf::TransferContext> output)
        -> coro<lf::ProtocolCompletion> {
        auto published = output.publish(context.clone());
        auto done      = co_await context.delay(Duration::from_secs(u64(20)));
        co_return lf::ProtocolCompletion { rstd::move(done), lf::RemoteOutcome::NotApplicable };
    }
    void request_stop(lf::StopReason) {}
    auto release(lf::ReleaseContext) -> coro<lf::Result<empty>> { co_return Ok(empty {}); }
};
auto metadata() -> h::CacheMetadata {
    auto                   req = request();
    lihttpto::ResponseHead head { lihttpto::StatusCode::make(u16(200)).unwrap(), {}, {}, {} };
    head.headers.add("Cache-Control"_str, "max-age=60"_str).unwrap();
    auto policy = lihttpto::CachePolicy::make(head.headers, h::cache_now(), h::cache_now());
    return { rstd::move(req.parts),
             rstd::move(head),
             {},
             rstd::move(policy),
             Bytes::copy_from_slice("opaque variant"_str.as_bytes()) };
}
} // namespace
TEST(HttpCache, InvalidationConflictsWithInflightCommitAndOldValidationSnapshot) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        auto engine = lf::Engine::make(runtime.handle()).unwrap();
        auto transfer =
            engine.submit(lf::PreparedOperation<lf::TransferContext>::make(ContextDriver {}))
                .unwrap();
        auto        context = (co_await transfer.output()).unwrap();
        auto        cache   = h::MemoryCache::make(engine.resources()).unwrap();
        h::CacheKey key {
            String::make("http://example.test/item"_str), String::make("GET"_str), {}, { {}, {} }
        };
        auto generation = (co_await cache.lookup(key.clone(), context.clone())).unwrap().generation;
        auto writer     = (co_await cache.begin_write({ key.clone(), generation, metadata(), {} },
                                                      context.clone()))
                              .unwrap();
        EXPECT_TRUE(
            (co_await writer.write(Bytes::copy_from_slice("abc"_str.as_bytes()), context.clone()))
                .is_ok());
        EXPECT_TRUE(
            (co_await cache.invalidate(
                 { key.target.clone(), key.partition.clone(), key.scope.clone() }, context.clone()))
                .is_ok());
        EXPECT_EQ((co_await writer.commit({}, context.clone())).unwrap(),
                  h::CommitResult::Conflict);
        EXPECT_TRUE((co_await writer.abort()).is_ok());
        EXPECT_TRUE(
            (co_await cache.lookup(key.clone(), context.clone())).unwrap().candidates.is_empty());
        generation = (co_await cache.lookup(key.clone(), context.clone())).unwrap().generation;
        auto good  = (co_await cache.begin_write({ key.clone(), generation, metadata(), {} },
                                                 context.clone()))
                         .unwrap();
        EXPECT_TRUE(
            (co_await good.write(Bytes::copy_from_slice("abc"_str.as_bytes()), context.clone()))
                .is_ok());
        EXPECT_EQ((co_await good.commit({}, context.clone())).unwrap(), h::CommitResult::Published);
        auto lookup   = (co_await cache.lookup(key.clone(), context.clone())).unwrap();
        auto snapshot = lookup.candidates[usize()].clone();
        auto a        = (co_await snapshot.open(context.clone())).unwrap(),
             b        = (co_await snapshot.open(context.clone())).unwrap();
        EXPECT_EQ((co_await a.next()).unwrap().unwrap().as_slice(), "abc"_str.as_bytes());
        EXPECT_EQ((co_await b.next()).unwrap().unwrap().as_slice(), "abc"_str.as_bytes());
        EXPECT_TRUE(
            (co_await cache.invalidate(
                 { key.target.clone(), key.partition.clone(), key.scope.clone() }, context.clone()))
                .is_ok());
        EXPECT_EQ(
            (co_await cache.update(snapshot.id(), lookup.generation, metadata(), context.clone()))
                .unwrap(),
            h::CommitResult::Conflict);
        EXPECT_TRUE((co_await a.next()).unwrap().is_none());
        EXPECT_TRUE((co_await b.next()).unwrap().is_none());
        EXPECT_TRUE((co_await a.close()).is_ok());
        EXPECT_TRUE((co_await b.close()).is_ok());
        (void)co_await transfer.control().close();
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(run());
}

namespace
{
auto peer_url(ref<str> path) -> String {
    return rstd::format(
        "http://127.0.0.1:{}{}", u16(::atoi(::getenv("LIFETCHTO_PEER_PORT"))), path);
}
} // namespace
TEST(PolicyPeer, RetryAndKnownLengthTrailersReplayOnTheWire) {
    if (! ::getenv("LIFETCHTO_PEER_PORT")) GTEST_SKIP();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        auto            engine  = lf::Engine::make(runtime.handle()).unwrap();
        auto            backend = h::NativeBackend::make(engine.resources()).unwrap();
        h::ClientConfig config;
        config.policy          = retry_policy();
        config.policy.redirect = Some(h::RedirectPolicy {});
        auto client            = h::Client::make(engine.handle(),
                                                 h::BackendBinding::Shared(backend.backend()),
                                                 rstd::move(config))
                                     .unwrap();
        auto retry = client.execute(request(peer_url("/policy-retry"_str).as_str())).unwrap();
        EXPECT_EQ((co_await finish(retry)).unwrap().as_slice(), "hello"_str.as_bytes());
        EXPECT_EQ((co_await retry.control().wait())->progress.attempts, usize(3));
        auto              req = request(peer_url("/policy-307"_str).as_str(), "POST"_str);
        lihttpto::Headers trailers;
        trailers.add("X-End"_str, "yes"_str).unwrap();
        req.body = h::UploadPlan::bytes(Bytes::copy_from_slice("abc"_str.as_bytes()),
                                        Some(rstd::move(trailers)));
        req.parts.headers.add("X-Dup"_str, "a"_str).unwrap();
        req.parts.headers.add("X-Dup"_str, "b"_str).unwrap();
        array<u8, 1> raw { u8(255) };
        req.parts.headers.add("X-Bin"_str, lihttpto::HeaderValue::make(raw.as_slice()).unwrap())
            .unwrap();
        auto replay = client.execute(rstd::move(req)).unwrap();
        EXPECT_EQ((co_await finish(replay)).unwrap().as_slice(), "abc"_str.as_bytes());
        auto rewrite   = request(peer_url("/policy-303"_str).as_str(), "POST"_str);
        rewrite.body   = h::UploadPlan::bytes(Bytes::copy_from_slice("abc"_str.as_bytes()));
        auto rewritten = client.execute(rstd::move(rewrite)).unwrap();
        EXPECT_EQ((co_await finish(rewritten)).unwrap().as_slice(), "hello"_str.as_bytes());
        EXPECT_TRUE((co_await client.close()).is_ok());
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(run());
}
TEST(PolicyPeer, LastModifiedVaryInvalidationAndOversizeCaching) {
    if (! ::getenv("LIFETCHTO_PEER_PORT")) GTEST_SKIP();
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        auto                  engine  = lf::Engine::make(runtime.handle()).unwrap();
        auto                  backend = h::NativeBackend::make(engine.resources()).unwrap();
        h::MemoryCacheOptions sizes;
        sizes.max_entry_bytes = usize(100);
        auto            cache = h::MemoryCache::make(engine.resources(), sizes).unwrap();
        h::ClientConfig config;
        config.policy.cache = h::CacheMode::Default;
        config.cache        = Some(h::StoreBinding::Shared(cache.store()));
        auto client         = h::Client::make(engine.handle(),
                                              h::BackendBinding::Shared(backend.backend()),
                                              rstd::move(config))
                                  .unwrap();
        for (int i = 0; i < 3; ++i) {
            auto transfer = client.execute(request(peer_url("/policy-date"_str).as_str())).unwrap();
            EXPECT_EQ((co_await finish(transfer)).unwrap().as_slice(), "hello"_str.as_bytes());
        }
        for (int i = 0; i < 3; ++i) {
            auto req = request(peer_url("/policy-vary"_str).as_str());
            req.parts.headers.add("X-Tone"_str, i == 2 ? "b"_str : "a"_str).unwrap();
            req.parts.headers.add("X-Tone"_str, i == 2 ? "a"_str : "b"_str).unwrap();
            auto transfer = client.execute(rstd::move(req)).unwrap();
            EXPECT_TRUE((co_await finish(transfer)).is_ok());
        }
        for (int i = 0; i < 2; ++i) {
            auto transfer =
                client.execute(request(peer_url("/policy-oversize"_str).as_str())).unwrap();
            EXPECT_EQ((co_await finish(transfer)).unwrap().len(), usize(1024));
        }
        for (int i = 0; i < 4; ++i) {
            auto transfer = client
                                .execute(request(peer_url("/policy-invalidate"_str).as_str(),
                                                 i == 2 ? "POST"_str : "GET"_str))
                                .unwrap();
            EXPECT_TRUE((co_await finish(transfer)).is_ok());
        }
        EXPECT_TRUE((co_await client.close()).is_ok());
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(run());
}
TEST(HttpPolicy, RedirectRefusesDowngradeLoopAndHopLimit) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        for (int mode = 0; mode < 3; ++mode) {
            auto engine = lf::Engine::make(runtime.handle()).unwrap();
            auto script = Script::make(Scenario {});
            {
                auto f = script->lock().unwrap();
                f->replies.push(redirection(
                    u16(302), mode == 0 ? "http://example.test/item"_str : "/item"_str));
                f->replies.push(redirection(u16(302), "/item"_str));
            }
            h::ClientConfig   config;
            h::RedirectPolicy redirect;
            redirect.allow_cross_origin = true;
            if (mode == 2) redirect.max_hops = usize();
            config.policy.redirect = Some(rstd::move(redirect));
            auto client =
                h::Client::make(engine.handle(), binding(script.clone()), rstd::move(config))
                    .unwrap();
            auto transfer = client
                                .execute(request(mode == 0 ? "https://example.test/item"_str
                                                           : "http://example.test/item"_str))
                                .unwrap();
            auto result   = co_await finish(transfer);
            EXPECT_TRUE(result.as_ref().unwrap_err().is_Redirect());
            EXPECT_EQ(script->lock().unwrap()->seen.len(), mode == 1 ? usize(2) : usize(1));
            EXPECT_TRUE((co_await client.close()).is_ok());
            EXPECT_TRUE((co_await engine.close()).is_ok());
        }
        co_return empty {};
    };
    runtime.block_on(run());
}
TEST(HttpCache, CacheRequiresStoreAndSnapshotsStopWithTheirTransfer) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        auto            engine = lf::Engine::make(runtime.handle()).unwrap();
        auto            script = Script::make(Scenario {});
        h::ClientConfig config;
        config.policy.cache = h::CacheMode::Default;
        EXPECT_TRUE(
            h::Client::make(engine.handle(), binding(script.clone()), rstd::move(config)).is_err());
        auto context_owner =
            engine.submit(lf::PreparedOperation<lf::TransferContext>::make(ContextDriver {}))
                .unwrap();
        auto        context = (co_await context_owner.output()).unwrap();
        auto        cache   = h::MemoryCache::make(engine.resources()).unwrap();
        h::CacheKey key {
            String::make("http://example.test/item"_str), String::make("GET"_str), {}, { {}, {} }
        };
        auto generation = (co_await cache.lookup(key.clone(), context.clone())).unwrap().generation;
        auto writer     = (co_await cache.begin_write({ key.clone(), generation, metadata(), {} },
                                                      context.clone()))
                              .unwrap();
        EXPECT_EQ((co_await writer.commit({}, context.clone())).unwrap(),
                  h::CommitResult::Published);
        auto entry = (co_await cache.lookup(key.clone(), context.clone()))
                         .unwrap()
                         .candidates[usize()]
                         .clone();
        auto body  = (co_await entry.open(context.clone())).unwrap();
        (void)co_await context_owner.control().close();
        EXPECT_TRUE((co_await body.next()).is_err());
        EXPECT_TRUE((co_await body.completion().wait()).is_err());
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(run());
}
TEST(HttpPolicy, AttemptHistoryIsBoundedAndKeepsTotalCount) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        auto engine = lf::Engine::make(runtime.handle()).unwrap();
        auto script = Script::make(Scenario {});
        {
            auto f = script->lock().unwrap();
            for (int i = 0; i < 64; ++i) {
                Reply reply;
                reply.status = u16(503);
                f->replies.push(rstd::move(reply));
            }
        }
        h::ClientConfig config;
        config.policy                       = retry_policy();
        config.policy.retry->max_retries    = usize(64);
        config.policy.retry->status_retries = usize(64);
        config.policy.retry->backoff        = Duration {};
        auto client =
            h::Client::make(engine.handle(), binding(script.clone()), rstd::move(config)).unwrap();
        h::ExecutionObserver observer;
        h::ExecuteOptions    options;
        options.transfer.max_attempts = usize(65);
        options.report                = Some(observer.clone());
        auto transfer                 = client.execute(request(), rstd::move(options)).unwrap();
        EXPECT_TRUE((co_await finish(transfer)).is_ok());
        auto report = (co_await observer.wait()).unwrap();
        EXPECT_EQ(report.facts.attempts, usize(65));
        EXPECT_EQ(report.facts.history.len(), usize(64));
        EXPECT_TRUE(report.facts.history_truncated);
        EXPECT_TRUE((co_await client.close()).is_ok());
        EXPECT_TRUE((co_await engine.close()).is_ok());
        co_return empty {};
    };
    runtime.block_on(run());
}

namespace
{
struct DeadlineRoute {
    Arc<rstd::sync::Mutex<Option<lf::Instant>>> observed;
    auto select(lihttpto::RequestParts, lf::TransferContext context) const
        -> coro<lf::Result<h::RouteChoice>> {
        *observed->lock().unwrap() = context.deadline();
        auto delayed               = co_await context.delay(Duration::from_millis(u64(20)));
        if (delayed.is_err()) co_return Err(rstd::move(delayed).unwrap_err());
        co_return Ok(h::RouteChoice {});
    }
    void request_close() const {}
    auto close() const -> coro<lf::Result<empty>> { co_return Ok(empty {}); }
};
} // namespace
namespace rstd
{
template<>
struct Impl<h::RouteProvider, DeadlineRoute> : ImplBase<DeadlineRoute> {
    auto select(lihttpto::RequestParts p, lf::TransferContext c) const {
        return this->self().select(rstd::move(p), rstd::move(c));
    }
    void request_close() const { this->self().request_close(); }
    auto close() const { return this->self().close(); }
};
} // namespace rstd
TEST(HttpPolicy, RequestTotalTimeoutOverridesDefaultAndKeepsHardDeadline) {
    auto runtime = rstd::async::RuntimeBuilder::current_thread().enable_all().build().unwrap();
    auto run     = [&]() -> coro<empty> {
        for (int mode = 0; mode < 6; ++mode) {
            auto            engine = lf::Engine::make(runtime.handle()).unwrap();
            auto            script = Script::make(Scenario {});
            h::ClientConfig config;
            config.total_timeout = Some(Duration::from_millis(u64(3)));
            auto client =
                h::Client::make(engine.handle(), binding(script.clone()), rstd::move(config))
                    .unwrap();
            auto deadline = Arc<rstd::sync::Mutex<Option<lf::Instant>>>::make(None<lf::Instant>());
            h::ExecuteOptions options;
            options.routing.shared_provider =
                Some(Arc<rstd::dyn<h::RouteProvider>>::make(DeadlineRoute { deadline.clone() }));
            if (mode == 1 || mode == 3) options.total_timeout = Some(h::TotalTimeout::disabled());
            if (mode == 2)
                options.total_timeout = Some(h::TotalTimeout::after(Duration::from_secs(u64(1))));
            if (mode == 3)
                options.transfer.deadline =
                    Some(lf::Instant::now() + Duration::from_millis(u64(3)));
            if (mode == 4)
                options.total_timeout = Some(h::TotalTimeout::after(Duration::from_millis(u64(1))));
            if (mode == 5) options.total_timeout = Some(h::TotalTimeout::after(Duration {}));
            auto submitted = client.execute(request(), options.clone());
            if (mode == 5) {
                EXPECT_TRUE(submitted.is_err());
                if (submitted.is_err()) {
                    auto rejected = rstd::move(submitted).unwrap_err();
                    EXPECT_TRUE(rejected.error.is_InvalidOptions());
                    EXPECT_TRUE((co_await rejected.input.body.close()).is_ok());
                }
            } else {
                auto transfer = rstd::move(submitted).unwrap();
                auto result   = co_await finish(transfer);
                if (mode == 1 || mode == 2)
                    EXPECT_TRUE(result.is_ok());
                else {
                    EXPECT_TRUE(result.is_err());
                    if (result.is_err()) EXPECT_TRUE(result.unwrap_err().is_Timeout());
                }
                if (mode == 1) EXPECT_TRUE(deadline->lock().unwrap()->is_none());
            }
            EXPECT_TRUE((co_await client.close()).is_ok());
            EXPECT_TRUE((co_await engine.close()).is_ok());
        }
        co_return empty {};
    };
    runtime.block_on(run());
}
