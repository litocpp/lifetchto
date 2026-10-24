module lifetchto.http;
import :client;
using namespace rstd::prelude;
using namespace rstd::literals;
namespace lifetchto::http
{
auto observe(Arc<ExecutionObserverState> state) -> coro<Result<ExecutionReport>> {
    Option<TransferControl> control;
    {
        auto f = state->fields.lock().unwrap();
        if (f->control.is_none()) co_return Err(Error::Closed());
        control = Some(f->control->clone());
    }
    auto report = co_await control->wait();
    auto f      = state->fields.lock().unwrap();
    co_return Ok(ExecutionReport { f->facts.clone(), rstd::move(report) });
}
auto ExecutionObserver::wait() const -> coro<Result<ExecutionReport>> {
    return observe(state_.clone());
}
struct ClientFields {
    usize               handles { 1 };
    bool                closed {};
    Option<CloseReport> close_report;
};
struct ClientState {
    TransferScope                   scope;
    BackendBinding                  binding;
    Arc<rstd::dyn<ExchangeBackend>> backend;
    ClientConfig                    config;
    rstd::sync::Mutex<ClientFields> fields { ClientFields {} };
    ClientState(TransferScope scope, BackendBinding binding, ClientConfig config)
        : scope(rstd::move(scope)),
          binding(rstd::move(binding)),
          backend(this->binding.backend()),
          config(rstd::move(config)) {}
};
auto Client::capabilities() const -> BackendCapabilities {
    return state_->backend->capabilities();
}
void stop_http_client(const Arc<ClientState>& state) {
    if (! state) return;
    {
        auto f    = state->fields.lock().unwrap();
        f->closed = true;
    }
    state->scope.request_close();
    if (state->binding.is_Owned()) state->backend->request_close();
    if (state->config.cache.is_some() && state->config.cache->is_Owned())
        state->config.cache->store()->request_close();
    if (state->config.cookies.is_some() && state->config.cookies->is_Owned())
        state->config.cookies->jar()->request_close();
}
void drop_http_client(const Arc<ClientState>& state) {
    if (! state) return;
    bool last;
    {
        auto f = state->fields.lock().unwrap();
        last   = --f->handles == usize();
    }
    if (last) stop_http_client(state);
}
auto finish_http_client(Arc<ClientState> state) -> coro<Result<empty>> {
    stop_http_client(state);
    auto report = co_await state->scope.close();
    if (state->binding.is_Owned()) {
        auto closed = co_await state->backend->close();
        if (closed.is_err()) report.add(rstd::move(closed).unwrap_err());
    }
    if (state->config.cache.is_some() && state->config.cache->is_Owned()) {
        auto closed = co_await state->config.cache->store()->close();
        if (closed.is_err()) report.add(rstd::move(closed).unwrap_err());
    }
    if (state->config.cookies.is_some() && state->config.cookies->is_Owned()) {
        auto closed = co_await state->config.cookies->jar()->close();
        if (closed.is_err()) report.add(rstd::move(closed).unwrap_err());
    }
    Option<Error> error;
    if (report.is_err()) error = Some(report.errors[usize()].clone());
    {
        auto f          = state->fields.lock().unwrap();
        f->close_report = Some(rstd::move(report));
    }
    if (error.is_some()) co_return Err(error.take().unwrap());
    co_return Ok(empty {});
}
struct ClientOwner {
    Arc<ClientState> state;
    void             request_stop(StopReason) const { stop_http_client(state); }
    auto             release(ReleaseContext) const -> coro<Result<empty>> {
        return finish_http_client(state.clone());
    }
};
struct BodyOwner {
    BodyControl control;
    void        request_stop(StopReason reason) const { control.cancel(reason); }
    auto        release(ReleaseContext) const -> coro<Result<empty>> { return control.close(); }
};
struct LogicalFields {
    Option<BodyControl> body;
};
struct LogicalState {
    Arc<ClientState>                 client;
    Option<ExecutionRequest>         request;
    ExecuteOptions                   options;
    ExecutionPolicy                  policy;
    Arc<ExecutionObserverState>      report;
    Option<Error>                    release_error;
    rstd::sync::Mutex<LogicalFields> fields { LogicalFields {} };
    LogicalState(Arc<ClientState>            client,
                 ExecutionRequest            request,
                 ExecuteOptions              options,
                 ExecutionPolicy             policy,
                 Arc<ExecutionObserverState> report)
        : client(rstd::move(client)),
          request(Some(rstd::move(request))),
          options(rstd::move(options)),
          policy(rstd::move(policy)),
          report(rstd::move(report)) {}
};
auto equal_fields(const lihttpto::Headers& a, const lihttpto::Headers& b, ref<str> name) -> bool {
    auto left = a.get_all(name), right = b.get_all(name);
    if (left.len() != right.len()) return false;
    for (usize i; i < left.len(); ++i)
        if (left[i]->as_slice() != right[i]->as_slice()) return false;
    return true;
}
auto cache_key(const PreparedExchange& prepared, ref<str> partition) -> CacheKey {
    const auto& parts = prepared.parts();
    return { rstd::format("{}{}", parts.url.http_origin()->serialize(), parts.url.request_target()),
             String::make(parts.method.as_ref()),
             String::make(partition),
             prepared.cache_scope().clone() };
}
auto caching_request(const lihttpto::RequestParts& parts,
                     const UploadMetadata&         upload,
                     const lihttpto::CacheControl& fields) -> bool {
    return (parts.method.as_ref() == "GET"_str || parts.method.as_ref() == "HEAD"_str) &&
           upload.length == Some(u64()) && ! fields.no_store && ! fields.invalid &&
           ! parts.headers.contains("authorization"_str) &&
           ! parts.headers.contains("cookie"_str) && ! parts.headers.contains("range"_str) &&
           ! parts.headers.contains("if-none-match"_str) &&
           ! parts.headers.contains("if-modified-since"_str);
}
auto variant_key(const lihttpto::CacheFields& fields, const lihttpto::Headers& request) -> Bytes {
    Vec<u8> bytes;
    for (const auto& name : fields.vary) {
        auto prefix = rstd::format("{}:", name.len());
        bytes.extend_from_slice(prefix.as_str().as_bytes());
        bytes.extend_from_slice(name.as_str().as_bytes());
        auto values = request.get_all(name.as_str());
        prefix      = rstd::format("{}:", values.len());
        bytes.extend_from_slice(prefix.as_str().as_bytes());
        for (auto value : values) {
            prefix = rstd::format("{}:", value->as_slice().len());
            bytes.extend_from_slice(prefix.as_str().as_bytes());
            bytes.extend_from_slice(value->as_slice());
        }
    }
    return Bytes::copy_from_slice(bytes.as_slice());
}
void record_attempt(const Arc<LogicalState>&      state,
                    const lihttpto::RequestParts& parts,
                    Option<u16>                   status,
                    const Arc<TransferReport>&    report) {
    auto f = state->report->fields.lock().unwrap();
    ++f->facts.attempts;
    if (f->facts.history.len() < usize(64))
        f->facts.history.push({ String::make(parts.url.as_ref()),
                                status,
                                report->logical.remote,
                                report->progress,
                                report->logical.result.is_err()
                                    ? Some(report->logical.result.as_ref().unwrap_err().clone())
                                    : None<Error>() });
    else
        f->facts.history_truncated = true;
}
auto drain_response(ResponseBody body, usize limit, Duration timeout, TransferContext context)
    -> coro<empty> {
    auto reading = [body = rstd::move(body), limit]() mutable -> coro<Result<empty>> {
        usize read {};
        for (;;) {
            auto next = co_await body.next();
            if (next.is_err()) co_return Err(rstd::move(next).unwrap_err());
            if (next->is_none()) co_return Ok(empty {});
            if ((**next).len() > limit - read) {
                body.cancel();
                co_return Err(Error::TooLarge());
            }
            read += (**next).len();
        }
    };
    auto ready =
        co_await rstd::async::select(reading().into_future(), context.delay(timeout).into_future());
    co_return empty {};
}
auto retry_delay(const RetryPolicy& policy, usize retries, const lihttpto::ResponseHead* head)
    -> Result<Duration> {
    auto cap = policy.max_backoff.as_millis();
    auto ms  = rstd::cmp::min(policy.backoff.as_millis(), cap);
    for (usize i; i < retries && i < usize(32); ++i)
        ms = ms > cap / u64(2) ? cap : rstd::cmp::min(cap, ms * u64(2));
    if (! policy.jitter.is_zero()) {
        array<u8, 8> random {};
        auto         filled = rstd::random::fill_secure(random.as_mut_slice());
        if (filled.is_err()) return Err(Error::Connect(rstd::move(filled).unwrap_err()));
        u64 value {};
        for (auto byte : random)
            value = (value << u64(8)) | u64(static_cast<u8>(byte).to_primitive());
        auto extra = value % (policy.jitter.as_millis() + u64(1));
        ms         = extra > cap - ms ? cap : ms + extra;
    }
    auto wait = Duration::from_millis(ms);
    if (head && policy.respect_retry_after) {
        auto parsed = lihttpto::retry_after(head->headers);
        if (parsed.is_ok() && parsed->is_some()) {
            auto seconds = (**parsed).delay(cache_now());
            auto server  = Duration::from_secs(u64(seconds.to_primitive()));
            if (server > wait) wait = server;
        }
    }
    return Ok(wait);
}
auto method_allowed(const RetryPolicy& policy, const lihttpto::Method& method) -> bool {
    if (policy.business_idempotent) return true;
    for (const auto& text : policy.methods)
        if (text.as_str() == method.as_ref()) return true;
    return false;
}
auto status_allowed(const RetryPolicy& policy, u16 status) -> bool {
    for (auto value : policy.statuses)
        if (value == status) return true;
    return false;
}
auto decide_retry(const RetryPolicy&            policy,
                  const lihttpto::RequestParts& parts,
                  const Error*                  error,
                  const lihttpto::ResponseHead* head,
                  RemoteOutcome                 remote,
                  usize                         retries,
                  usize                         classified) -> Result<RetryDecision> {
    if (retries >= policy.max_retries) return Ok(RetryDecision {});
    bool retry = false;
    if (error)
        retry = classified < policy.connect_retries &&
                (error->is_Connect() || error->is_Resolve()) &&
                (remote == RemoteOutcome::NotDispatched || method_allowed(policy, parts.method));
    else
        retry = head && classified < policy.status_retries &&
                status_allowed(policy, head->status.value()) &&
                method_allowed(policy, parts.method);
    if (! retry && policy.decider.is_none()) return Ok(RetryDecision {});
    auto delay = retry_delay(policy, retries, head);
    if (delay.is_err()) return Err(rstd::move(delay).unwrap_err());
    if (policy.decider.is_some())
        return (*policy.decider)->decide({ parts, error, head, remote, retries, *delay });
    return Ok(RetryDecision { retry, *delay });
}
struct TapFields {
    bool active { true }, canceled {};
};
struct TapState {
    ResponseBody                 body;
    Arc<CacheWriter>             writer;
    TransferControl              attempt;
    TransferContext              context;
    StoreFailure                 failure;
    rstd::sync::Mutex<TapFields> fields { TapFields {} };
    TapState(ResponseBody    body,
             CacheWriter     writer,
             TransferControl attempt,
             TransferContext context,
             StoreFailure    failure)
        : body(rstd::move(body)),
          writer(Arc<CacheWriter>::make(rstd::move(writer))),
          attempt(rstd::move(attempt)),
          context(rstd::move(context)),
          failure(failure) {}
};
auto tap_next(Arc<TapState> state) -> coro<Result<Option<Bytes>>> {
    auto result = co_await state->body.next();
    if (result.is_err()) {
        state->writer->request_abort();
        co_return Err(rstd::move(result).unwrap_err());
    }
    bool active;
    {
        auto f = state->fields.lock().unwrap();
        active = f->active && ! f->canceled;
    }
    if (! active) co_return rstd::move(result);
    if (result->is_some()) {
        auto written = co_await state->writer->write((**result).clone(), state->context.clone());
        if (written.is_err()) {
            {
                auto f    = state->fields.lock().unwrap();
                f->active = false;
            }
            state->writer->request_abort();
            if (state->failure == StoreFailure::Fail) {
                state->body.cancel();
                co_return Err(rstd::move(written).unwrap_err());
            }
        }
    } else {
        auto report = co_await state->attempt.wait();
        if (report->logical.result.is_err()) {
            state->writer->request_abort();
            co_return Err(report->logical.result.as_ref().unwrap_err().clone());
        }
        auto trailers = state->body.trailers();
        if (trailers.is_err()) {
            state->writer->request_abort();
            if (trailers.as_ref().unwrap_err().is_Unsupported()) {
                (void)co_await state->writer->abort();
                state->fields.lock().unwrap()->active = false;
                co_return rstd::move(result);
            }
            co_return Err(rstd::move(trailers).unwrap_err());
        }
        auto committed = co_await state->writer->commit(trailers->is_some() ? rstd::move(**trailers)
                                                                            : lihttpto::Headers {},
                                                        state->context.clone());
        {
            auto f    = state->fields.lock().unwrap();
            f->active = false;
        }
        if (committed.is_err() || *committed == CommitResult::Conflict) {
            (void)co_await state->writer->abort();
            if (committed.is_err() && state->failure == StoreFailure::Fail)
                co_return Err(rstd::move(committed).unwrap_err());
        }
    }
    co_return rstd::move(result);
}
struct TapSource {
    Arc<TapState> state;
    auto          next() const -> coro<Result<Option<Bytes>>> { return tap_next(state.clone()); }
    auto trailers() const -> Result<Option<lihttpto::Headers>> { return state->body.trailers(); }
    void cancel() const {
        {
            auto f      = state->fields.lock().unwrap();
            f->canceled = true;
            f->active   = false;
        }
        state->writer->request_abort();
        state->body.cancel();
    }
    auto close() const -> coro<Result<empty>> {
        auto closed  = co_await state->body.close();
        auto aborted = co_await state->writer->abort();
        if (closed.is_err()) co_return Err(rstd::move(closed).unwrap_err());
        co_return rstd::move(aborted);
    }
};
struct LogicalDriver {
    Arc<LogicalState> state;
    auto              run(TransferContext context, OutputSender<ResponseMessage> output)
        -> coro<ProtocolCompletion>;
    void request_stop(StopReason reason) {
        Option<BodyControl> body;
        {
            auto f = state->fields.lock().unwrap();
            if (f->body.is_some()) body = Some(f->body->clone());
        }
        if (body.is_some()) body->cancel(reason);
    }
    auto release(ReleaseContext) -> coro<Result<empty>> {
        if (state->request.is_some()) {
            auto closed = co_await state->request->body.close();
            if (closed.is_err() && state->release_error.is_none())
                state->release_error = Some(rstd::move(closed).unwrap_err());
        }
        if (state->release_error.is_some()) co_return Err(state->release_error->clone());
        co_return Ok(empty {});
    }
};
} // namespace lifetchto::http
namespace rstd
{
template<>
struct Impl<lifetchto::ManagedResource, lifetchto::http::ClientOwner>
    : ImplBase<lifetchto::http::ClientOwner> {
    void request_stop(lifetchto::StopReason reason) const { this->self().request_stop(reason); }
    auto release(lifetchto::ReleaseContext context) const { return this->self().release(context); }
};
template<>
struct Impl<lifetchto::ManagedResource, lifetchto::http::BodyOwner>
    : ImplBase<lifetchto::http::BodyOwner> {
    void request_stop(lifetchto::StopReason reason) const { this->self().request_stop(reason); }
    auto release(lifetchto::ReleaseContext context) const { return this->self().release(context); }
};
template<>
struct Impl<lifetchto::http::ResponseSource, lifetchto::http::TapSource>
    : ImplBase<lifetchto::http::TapSource> {
    auto next() const { return this->self().next(); }
    auto trailers() const { return this->self().trailers(); }
    void cancel() const { this->self().cancel(); }
    auto close() const { return this->self().close(); }
};
} // namespace rstd
namespace lifetchto::http
{
auto publish_response(const Arc<LogicalState>&      state,
                      TransferContext               context,
                      OutputSender<ResponseMessage> output,
                      ResponseMessage               message,
                      bool                          cached) -> coro<ProtocolCompletion> {
    auto control = message.body.completion();
    {
        auto f  = state->fields.lock().unwrap();
        f->body = Some(control.clone());
    }
    auto registered = context.resources().attach(
        empty {}, Arc<rstd::dyn<ManagedResource>>::make(BodyOwner { control.clone() }));
    if (registered.is_err()) {
        auto rejected = rstd::move(registered).unwrap_err();
        rejected.input.request_close();
        (void)co_await rejected.input.close();
        co_return ProtocolCompletion { Err(rstd::move(rejected.error)), RemoteOutcome::Unknown };
    }
    auto owner = rstd::move(registered).unwrap();
    {
        auto f                 = state->report->fields.lock().unwrap();
        f->facts.from_cache    = cached;
        f->facts.head_received = Some(Instant::now());
    }
    auto published = output.publish(rstd::move(message));
    if (published.is_err()) control.cancel();
    auto completed = co_await control.wait();
    co_return ProtocolCompletion { rstd::move(completed),
                                   cached ? RemoteOutcome::NotApplicable
                                          : RemoteOutcome::Confirmed };
}
auto cached_response(const Arc<LogicalState>&       state,
                     TransferContext                context,
                     OutputSender<ResponseMessage>  output,
                     CacheEntry                     entry,
                     Option<lihttpto::ResponseHead> changed = {}) -> coro<ProtocolCompletion> {
    auto head = changed.is_some() ? changed.take().unwrap() : entry.metadata().head.clone();
    auto age  = entry.metadata().policy.age(cache_now());
    head.headers.set("Age"_str, rstd::format("{}", age).as_str()).unwrap();
    auto body = co_await entry.open(context.clone());
    if (body.is_err())
        co_return ProtocolCompletion { Err(rstd::move(body).unwrap_err()),
                                       RemoteOutcome::NotApplicable };
    co_return co_await publish_response(state,
                                        context.clone(),
                                        rstd::move(output),
                                        { rstd::move(head), rstd::move(body).unwrap() },
                                        true);
}
auto LogicalDriver::run(TransferContext context, OutputSender<ResponseMessage> output)
    -> coro<ProtocolCompletion> {
    auto                               client  = state->client.clone();
    auto                               backend = client->backend.clone();
    Option<Arc<rstd::dyn<CacheStore>>> store;
    if (client->config.cache.is_some()) store = Some(client->config.cache->store());
    Option<Arc<rstd::dyn<CookieJar>>> cookies;
    if (state->options.cookies.is_Shared())
        cookies = Some(state->options.cookies.as_Shared().value.clone());
    else if (state->options.cookies.is_Inherit() && client->config.cookies.is_some())
        cookies = Some(client->config.cookies->jar());
    auto&                    request       = *state->request;
    bool                     manual_cookie = request.parts.headers.contains("cookie"_str);
    usize                    redirects {}, retries {}, connect_retries {}, status_retries {};
    Vec<String>              visited;
    Option<PreparedExchange> prepared_hop;
    Option<CacheKey>         hop_key;
    Option<CacheEntry>       cached;
    u64                      generation {};
    bool                     have_generation {}, eligible {};
    for (;;) {
        if (context.cancellation().is_canceled())
            co_return ProtocolCompletion { Err(stop_error(*context.cancellation().reason())),
                                           RemoteOutcome::Unknown };
        if (cookies.is_some() && ! manual_cookie) {
            auto selected = (*cookies)->select(request.parts.url, cache_now());
            if (selected.is_err())
                co_return ProtocolCompletion { Err(rstd::move(selected).unwrap_err()),
                                               RemoteOutcome::NotDispatched };
            auto encoded = selected->encode();
            auto current = request.parts.headers.get_unique_text("cookie"_str).unwrap();
            bool changed = current.is_some() != ! encoded.is_empty() ||
                           (current.is_some() && *current != encoded.as_str());
            if (changed) {
                request.parts.headers.remove("cookie"_str);
                if (! encoded.is_empty())
                    request.parts.headers.set("Cookie"_str, encoded.as_str()).unwrap();
                prepared_hop = None();
            }
        }
        if (prepared_hop.is_none()) {
            RouteChoice route { state->options.routing->route.clone() };
            if (state->options.routing->shared_provider.is_some()) {
                auto selected = co_await (*state->options.routing->shared_provider)
                                    ->select(request.parts.clone(), context.clone());
                if (selected.is_err())
                    co_return ProtocolCompletion { Err(rstd::move(selected).unwrap_err()),
                                                   RemoteOutcome::NotDispatched };
                route = rstd::move(selected).unwrap();
            }
            auto prepared_result =
                backend->prepare(request.parts.clone(), request.body.metadata(), rstd::move(route));
            if (prepared_result.is_err())
                co_return ProtocolCompletion { Err(rstd::move(prepared_result).unwrap_err()),
                                               RemoteOutcome::NotDispatched };
            auto prepared = rstd::move(prepared_result).unwrap();
            {
                auto f             = state->report->fields.lock().unwrap();
                f->facts.request   = Some(request.parts.clone());
                f->facts.redirects = redirects;
            }
            hop_key             = Some(cache_key(prepared, state->options.partition.as_str()));
            auto& key           = *hop_key;
            auto  request_cache = lihttpto::CacheControl::parse(request.parts.headers);
            eligible   = state->policy.cache != CacheMode::Disabled &&
                         state->policy.cache != CacheMode::Bypass &&
                         caching_request(request.parts, request.body.metadata(), request_cache);
            cached     = None();
            generation = u64();
            have_generation = false;
            if (eligible && store.is_some()) {
                auto lookup = co_await (*store)->lookup(key.clone(), context.clone());
                if (lookup.is_err()) {
                    if (client->config.store_failure == StoreFailure::Fail)
                        co_return ProtocolCompletion { Err(rstd::move(lookup).unwrap_err()),
                                                       RemoteOutcome::NotApplicable };
                } else {
                    generation      = lookup->generation;
                    have_generation = true;
                    for (const auto& entry : lookup->candidates)
                        if (entry.metadata().policy.matches_request([&](ref<str> name) {
                                return equal_fields(
                                    request.parts.headers, entry.metadata().request.headers, name);
                            })) {
                            cached = Some(entry.clone());
                            break;
                        }
                }
            }
            if (cached.is_some() && state->policy.cache != CacheMode::Revalidate &&
                cached->metadata().policy.fresh(cache_now(), request_cache))
                co_return co_await cached_response(
                    state, context.clone(), rstd::move(output), cached.take().unwrap());
            if (state->policy.cache == CacheMode::OnlyIfCached)
                co_return ProtocolCompletion { Err(Error::CacheMiss()),
                                               RemoteOutcome::NotApplicable };
            if (cached.is_some()) {
                auto fields      = cached->metadata().policy.fields.clone();
                auto conditional = request.parts.clone();
                bool validate {};
                if (fields.etag.is_some()) {
                    conditional.headers.set("If-None-Match"_str, fields.etag->as_str()).unwrap();
                    validate = true;
                } else if (fields.last_modified.is_some()) {
                    auto date = lihttpto::format_http_date(*fields.last_modified);
                    if (date.is_some()) {
                        conditional.headers.set("If-Modified-Since"_str, date->as_str()).unwrap();
                        validate = true;
                    }
                }
                if (validate) {
                    auto updated = backend->prepare(rstd::move(conditional),
                                                    request.body.metadata(),
                                                    prepared.target().clone());
                    if (updated.is_err())
                        co_return ProtocolCompletion { Err(rstd::move(updated).unwrap_err()),
                                                       RemoteOutcome::NotDispatched };
                    prepared = rstd::move(updated).unwrap();
                } else
                    cached = None();
            }
            prepared_hop = Some(rstd::move(prepared));
        }
        auto& prepared = *prepared_hop;
        auto& key      = *hop_key;
        auto  opened   = co_await request.body.open(context.clone());
        if (opened.is_err())
            co_return ProtocolCompletion { Err(rstd::move(opened).unwrap_err()),
                                           RemoteOutcome::NotDispatched };
        auto operation = backend->operation({ prepared.clone(), rstd::move(opened).unwrap() });
        if (operation.is_err()) {
            auto rejected = rstd::move(operation).unwrap_err();
            rejected.input.body.cancel();
            auto closed = co_await rejected.input.body.close();
            if (closed.is_err()) state->release_error = Some(rstd::move(closed).unwrap_err());
            co_return ProtocolCompletion { Err(rstd::move(rejected.error)),
                                           RemoteOutcome::NotDispatched };
        }
        auto submitted = context.submit(rstd::move(operation).unwrap());
        if (submitted.is_err()) {
            auto rejected = rstd::move(submitted).unwrap_err();
            (void)co_await rejected.input.close();
            co_return ProtocolCompletion { Err(rstd::move(rejected.error)),
                                           RemoteOutcome::NotDispatched };
        }
        auto attempt         = rstd::move(submitted).unwrap();
        auto attempt_control = attempt.control();
        auto requested       = cache_now();
        auto response        = co_await attempt.output();
        if (response.is_err()) {
            auto report = co_await attempt_control.wait();
            record_attempt(state, request.parts, {}, report);
            auto error = response.as_ref().unwrap_err().clone();
            if (state->policy.retry.is_some() && ! context.cancellation().is_canceled()) {
                auto decision = decide_retry(*state->policy.retry,
                                             request.parts,
                                             &error,
                                             nullptr,
                                             report->logical.remote,
                                             retries,
                                             connect_retries);
                if (decision.is_err())
                    co_return ProtocolCompletion { Err(rstd::move(decision).unwrap_err()),
                                                   report->logical.remote };
                if (decision->retry) {
                    if (! request.body.replayable())
                        co_return ProtocolCompletion { Err(Error::ReplayUnavailable()),
                                                       report->logical.remote };
                    ++retries;
                    ++connect_retries;
                    auto wait = co_await context.delay(decision->delay);
                    if (wait.is_err())
                        co_return ProtocolCompletion { Err(rstd::move(wait).unwrap_err()),
                                                       report->logical.remote };
                    continue;
                }
            }
            co_return ProtocolCompletion { Err(rstd::move(error)), report->logical.remote };
        }
        auto message = rstd::move(response).unwrap();
        auto status  = message.head.status.value();
        if (cookies.is_some()) {
            for (auto value : message.head.headers.get_all("set-cookie"_str)) {
                auto parsed = lihttpto::SetCookie::parse_bytes(value->as_slice());
                if (parsed.is_err()) continue;
                auto stored = (*cookies)->store(request.parts.url, *parsed, cache_now());
                if (stored.is_err()) {
                    message.body.cancel();
                    (void)co_await attempt_control.close();
                    co_return ProtocolCompletion { Err(rstd::move(stored).unwrap_err()),
                                                   RemoteOutcome::Confirmed };
                }
            }
        }
        if (request.parts.method.as_ref() != "GET"_str &&
            request.parts.method.as_ref() != "HEAD"_str &&
            request.parts.method.as_ref() != "OPTIONS"_str &&
            request.parts.method.as_ref() != "TRACE"_str && status >= u16(200) &&
            status < u16(400) && store.is_some()) {
            auto invalidated = co_await (*store)->invalidate(
                { key.target.clone(), key.partition.clone(), key.scope.clone() }, context.clone());
            if (invalidated.is_err() && client->config.store_failure == StoreFailure::Fail) {
                message.body.cancel();
                (void)co_await attempt_control.close();
                co_return ProtocolCompletion { Err(rstd::move(invalidated).unwrap_err()),
                                               RemoteOutcome::Confirmed };
            }
        }
        if (status == u16(304) && cached.is_some()) {
            co_await drain_response(
                rstd::move(message.body), usize(), client->config.drain_timeout, context.clone());
            auto report = co_await attempt_control.wait();
            record_attempt(state, request.parts, Some(status), report);
            if (report->logical.result.is_err())
                co_return ProtocolCompletion {
                    Err(report->logical.result.as_ref().unwrap_err().clone()),
                    report->logical.remote
                };
            auto metadata = cached->metadata().clone();
            metadata.head.headers =
                lihttpto::merge_not_modified(metadata.head.headers, message.head.headers);
            metadata.policy =
                lihttpto::CachePolicy::make(metadata.head.headers,
                                            requested,
                                            cache_now(),
                                            Some(i64(cached->metadata().policy.date_anchor)));
            metadata.request = prepared.parts().clone();
            metadata.variant = variant_key(metadata.policy.fields, metadata.request.headers);
            auto head        = metadata.head.clone();
            if (metadata.policy.fields.reusable && ! head.headers.contains("set-cookie"_str)) {
                auto updated = co_await (*store)->update(
                    cached->id(), generation, rstd::move(metadata), context.clone());
                if (updated.is_err() && client->config.store_failure == StoreFailure::Fail)
                    co_return ProtocolCompletion { Err(rstd::move(updated).unwrap_err()),
                                                   RemoteOutcome::Confirmed };
            } else {
                auto removed = co_await (*store)->invalidate(
                    { key.target.clone(), key.partition.clone(), key.scope.clone() },
                    context.clone());
                if (removed.is_err() && client->config.store_failure == StoreFailure::Fail)
                    co_return ProtocolCompletion { Err(rstd::move(removed).unwrap_err()),
                                                   RemoteOutcome::Confirmed };
            }
            co_return co_await cached_response(state,
                                               context.clone(),
                                               rstd::move(output),
                                               cached.take().unwrap(),
                                               Some(rstd::move(head)));
        }
        if (state->policy.redirect.is_some()) {
            auto target = lihttpto::redirect_target(message.head, request.parts.url);
            if (target.is_err()) {
                message.body.cancel();
                (void)co_await attempt_control.close();
                co_return ProtocolCompletion { Err(Error::Redirect(
                                                   String::make("invalid redirect target"_str))),
                                               RemoteOutcome::Confirmed };
            }
            if (target->is_some()) {
                auto& policy = *state->policy.redirect;
                auto  action = RedirectAction::Follow;
                if (policy.decider.is_some()) {
                    auto chosen =
                        (*policy.decider)
                            ->decide({ request.parts, message.head, **target, redirects });
                    if (chosen.is_err()) {
                        message.body.cancel();
                        (void)co_await attempt_control.close();
                        co_return ProtocolCompletion { Err(rstd::move(chosen).unwrap_err()),
                                                       RemoteOutcome::Confirmed };
                    }
                    action = *chosen;
                }
                if (action != RedirectAction::Return) {
                    auto next       = rstd::move(**target);
                    bool same       = request.parts.url.same_http_origin(next);
                    bool downgraded = request.parts.url.scheme() == Some("https"_str) &&
                                      next.scheme() == Some("http"_str);
                    bool loop       = false;
                    for (const auto& item : visited)
                        if (item.as_str() == next.as_ref()) loop = true;
                    if (action == RedirectAction::Fail || redirects >= policy.max_hops || loop ||
                        next.http_origin().is_none() || next.userinfo().is_some() ||
                        (! same && ! policy.allow_cross_origin) ||
                        (downgraded && ! policy.allow_https_downgrade)) {
                        message.body.cancel();
                        (void)co_await attempt_control.close();
                        co_return ProtocolCompletion {
                            Err(Error::Redirect(
                                String::make("redirect refused by policy or limit"_str))),
                            RemoteOutcome::Confirmed
                        };
                    }
                    bool get =
                        (status == u16(303) && request.parts.method.as_ref() != "HEAD"_str) ||
                        ((status == u16(301) || status == u16(302)) &&
                         request.parts.method.as_ref() == "POST"_str &&
                         policy.rewrite_post_301_302);
                    if (! get && ! request.body.replayable()) {
                        message.body.cancel();
                        (void)co_await attempt_control.close();
                        co_return ProtocolCompletion { Err(Error::ReplayUnavailable()),
                                                       RemoteOutcome::Confirmed };
                    }
                    co_await drain_response(rstd::move(message.body),
                                            client->config.drain_limit,
                                            client->config.drain_timeout,
                                            context.clone());
                    auto report = co_await attempt_control.wait();
                    record_attempt(state, request.parts, Some(status), report);
                    visited.push(String::make(request.parts.url.as_ref()));
                    request.parts.url = rstd::move(next);
                    if (! same) {
                        manual_cookie = false;
                        for (auto name : array<ref<str>, 4> { "authorization"_str,
                                                              "proxy-authorization"_str,
                                                              "cookie"_str,
                                                              "host"_str })
                            request.parts.headers.remove(name);
                        for (const auto& name : policy.remove_on_cross_origin)
                            request.parts.headers.remove(name.as_str());
                        if (state->policy.retry.is_some())
                            state->policy.retry->business_idempotent = false;
                    }
                    if (get) {
                        auto closed = co_await request.body.close();
                        if (closed.is_err())
                            state->release_error = Some(rstd::move(closed).unwrap_err());
                        request.body         = UploadPlan::empty();
                        request.parts.method = lihttpto::Method::parse("GET"_str).unwrap();
                        for (auto name : array<ref<str>, 5> { "content-length"_str,
                                                              "transfer-encoding"_str,
                                                              "content-type"_str,
                                                              "expect"_str,
                                                              "trailer"_str })
                            request.parts.headers.remove(name);
                    }
                    prepared_hop = None();
                    hop_key      = None();
                    cached       = None();
                    ++redirects;
                    continue;
                }
            }
        }
        if (state->policy.retry.is_some() && ! context.cancellation().is_canceled()) {
            auto decision = decide_retry(*state->policy.retry,
                                         request.parts,
                                         nullptr,
                                         &message.head,
                                         RemoteOutcome::Confirmed,
                                         retries,
                                         status_retries);
            if (decision.is_err()) {
                message.body.cancel();
                (void)co_await attempt_control.close();
                co_return ProtocolCompletion { Err(rstd::move(decision).unwrap_err()),
                                               RemoteOutcome::Confirmed };
            }
            if (decision->retry) {
                if (! request.body.replayable()) {
                    message.body.cancel();
                    (void)co_await attempt_control.close();
                    co_return ProtocolCompletion { Err(Error::ReplayUnavailable()),
                                                   RemoteOutcome::Confirmed };
                }
                co_await drain_response(rstd::move(message.body),
                                        client->config.drain_limit,
                                        client->config.drain_timeout,
                                        context.clone());
                auto report = co_await attempt_control.wait();
                record_attempt(state, request.parts, Some(status), report);
                ++retries;
                ++status_retries;
                auto wait = co_await context.delay(decision->delay);
                if (wait.is_err())
                    co_return ProtocolCompletion { Err(rstd::move(wait).unwrap_err()),
                                                   RemoteOutcome::Confirmed };
                continue;
            }
        }
        if (eligible && have_generation && store.is_some() && status == u16(200) &&
            ! message.head.headers.contains("set-cookie"_str)) {
            auto policy = lihttpto::CachePolicy::make(message.head.headers, requested, cache_now());
            if (policy.fields.reusable &&
                (policy.lifetime() > i64() || policy.fields.etag.is_some() ||
                 policy.fields.last_modified.is_some())) {
                auto variant = variant_key(policy.fields, request.parts.headers);
                auto writer  = co_await (*store)->begin_write({ key.clone(),
                                                                generation,
                                                                { request.parts.clone(),
                                                                  message.head.clone(),
                                                                  {},
                                                                  rstd::move(policy),
                                                                  rstd::move(variant) },
                                                                {} },
                                                              context.clone());
                if (writer.is_err()) {
                    if (client->config.store_failure == StoreFailure::Fail) {
                        message.body.cancel();
                        (void)co_await attempt_control.close();
                        co_return ProtocolCompletion { Err(rstd::move(writer).unwrap_err()),
                                                       RemoteOutcome::Confirmed };
                    }
                } else {
                    auto tap     = Arc<TapState>::make(rstd::move(message.body),
                                                       rstd::move(writer).unwrap(),
                                                       attempt_control.clone(),
                                                       context.clone(),
                                                       client->config.store_failure);
                    message.body = ResponseBody::from_source(
                        Arc<rstd::dyn<ResponseSource>>::make(TapSource { rstd::move(tap) }));
                }
            }
        }
        auto completion = co_await publish_response(
            state, context.clone(), rstd::move(output), rstd::move(message), false);
        auto report = co_await attempt_control.wait();
        record_attempt(state, request.parts, Some(status), report);
        if (completion.result.is_ok() && report->logical.result.is_err())
            completion.result = Err(report->logical.result.as_ref().unwrap_err().clone());
        co_return rstd::move(completion);
    }
}
auto valid_policy(const ExecutionPolicy& policy, bool store) -> bool {
    return (! policy.redirect.is_some() || policy.redirect->max_hops <= usize(64)) &&
           (! policy.retry.is_some() || (policy.retry->max_retries <= usize(64) &&
                                         policy.retry->jitter.as_millis() < u64(3600000))) &&
           (policy.cache == CacheMode::Disabled || policy.cache == CacheMode::Bypass || store);
}
auto ClientConfig::validate() const -> Result<empty> {
    if (! valid_policy(policy, cache.is_some()) || (cache.is_some() && ! cache->store()) ||
        (cookies.is_some() && ! cookies->jar()) ||
        (routing.route.is_some() && routing.shared_provider.is_some()) ||
        (total_timeout.is_some() && total_timeout->is_zero()))
        return Err(Error::InvalidOptions(String::make("invalid HTTP policy or cache binding"_str)));
    return Ok(empty {});
}
auto Client::make(EngineHandle engine, BackendBinding backend, ClientConfig config)
    -> Result<Client> {
    auto valid = config.validate();
    if (valid.is_err()) return Err(rstd::move(valid).unwrap_err());
    if (! backend.backend())
        return Err(Error::InvalidOptions(String::make("null HTTP backend"_str)));
    auto scope = engine.scope();
    if (scope.is_err()) return Err(rstd::move(scope).unwrap_err());
    auto state =
        Arc<ClientState>::make(rstd::move(scope).unwrap(), rstd::move(backend), rstd::move(config));
    auto resources = engine.resources();
    if (resources.is_err()) return Err(rstd::move(resources).unwrap_err());
    auto attached = resources->attach(
        empty {}, Arc<rstd::dyn<ManagedResource>>::make(ClientOwner { state.clone() }));
    if (attached.is_err()) return Err(rstd::move(attached).unwrap_err().error);
    return Ok(
        Client(rstd::move(state), Arc<ResourceLease<empty>>::make(rstd::move(attached).unwrap())));
}
Client::Client(Arc<ClientState> state, Arc<ResourceLease<empty>> registration)
    : state_(rstd::move(state)), registration_(rstd::move(registration)) {
}
Client::Client(Client&&) noexcept = default;
Client::~Client() {
    drop_http_client(state_);
}
auto Client::operator=(Client&& other) noexcept -> Client& {
    if (this != &other) {
        drop_http_client(state_);
        state_        = rstd::move(other.state_);
        registration_ = rstd::move(other.registration_);
    }
    return *this;
}
auto Client::clone() const -> Client {
    {
        auto f = state_->fields.lock().unwrap();
        ++f->handles;
    }
    return Client(state_.clone(), registration_.clone());
}
auto Client::policy() const -> ExecutionPolicy {
    return state_->config.policy.clone();
}
auto Client::resources() const -> ResourceContext {
    return state_->scope.resources();
}
auto resolve_transfer_options(TransferOptions      options,
                              Option<TotalTimeout> override_timeout,
                              Option<Duration>     default_timeout) -> Result<TransferOptions> {
    auto duration = override_timeout.is_some() ? override_timeout->duration() : default_timeout;
    if (duration.is_some() && duration->is_zero())
        return Err(Error::InvalidOptions(String::make("zero total timeout"_str)));
    if (duration.is_some()) {
        auto deadline = Instant::now() + *duration;
        if (options.deadline.is_none() || deadline < *options.deadline)
            options.deadline = Some(deadline);
    }
    return Ok(options);
}
auto Client::prepare_connection(DuplexRequest request, DuplexOptions options) const
    -> rstd::Result<ConnectionPlan, Rejected<DuplexRequest>> {
    if (state_->fields.lock().unwrap()->closed)
        return Err(Rejected<DuplexRequest> { Error::Closed(), rstd::move(request) });
    auto transfer_options = resolve_transfer_options(
        options.transfer, options.total_timeout, state_->config.total_timeout);
    if (transfer_options.is_err())
        return Err(Rejected<DuplexRequest> { rstd::move(transfer_options).unwrap_err(),
                                             rstd::move(request) });
    auto selected = request.clone();
    if (selected.route.route.is_none()) {
        if (state_->config.routing.shared_provider.is_some())
            return Err(Rejected<DuplexRequest> {
                Error::Unsupported(
                    "connection requires an explicit route with a dynamic route provider"_Str),
                rstd::move(request) });
        selected.route.route = state_->config.routing.route.clone();
    }
    return Ok(ConnectionPlan { rstd::move(selected), *transfer_options });
}
auto Client::duplex(DuplexRequest request, DuplexOptions options) const
    -> rstd::Result<PendingDuplex, Rejected<DuplexRequest>> {
    auto plan = prepare_connection(request.clone(), options);
    if (plan.is_err()) return Err(rstd::move(plan).unwrap_err());
    auto prepared = state_->backend->duplex(rstd::move(plan->request));
    if (prepared.is_err())
        return Err(Rejected<DuplexRequest> { rstd::move(prepared).unwrap_err().error,
                                             rstd::move(request) });
    auto submitted = state_->scope.submit(rstd::move(prepared).unwrap(), plan->transfer);
    if (submitted.is_err())
        return Err(Rejected<DuplexRequest> { rstd::move(submitted).unwrap_err().error,
                                             rstd::move(request) });
    return Ok(PendingDuplex(rstd::move(submitted).unwrap()));
}
auto Client::upgrade(UpgradeRequest request, UpgradeOptions options) const
    -> rstd::Result<PendingUpgrade, Rejected<UpgradeRequest>> {
    if (options.handshake_timeout.is_zero() || options.header_bytes == usize() ||
        options.header_bytes > usize(65536) || options.error_body_bytes > usize(65536) ||
        request.handshake.bytes().len() > usize(65536))
        return Err(Rejected<UpgradeRequest> { Error::InvalidOptions("invalid Upgrade limits"_Str),
                                              rstd::move(request) });
    auto pending = duplex(request.target.clone(), options.duplex);
    if (pending.is_err())
        return Err(Rejected<UpgradeRequest> { rstd::move(pending).unwrap_err().error,
                                              rstd::move(request) });
    return Ok(PendingUpgrade(rstd::move(pending).unwrap(), rstd::move(request.handshake), options));
}
auto Client::execute(ExecutionRequest request, ExecuteOptions options) const
    -> rstd::Result<Transfer<ResponseMessage>, Rejected<ExecutionRequest>> {
    {
        auto f = state_->fields.lock().unwrap();
        if (f->closed)
            return Err(Rejected<ExecutionRequest> { Error::Closed(), rstd::move(request) });
    }
    if (options.routing.is_none()) options.routing = Some(state_->config.routing.clone());
    auto supported = state_->backend->capabilities().validate(
        request.parts, request.body.metadata(), { options.routing->route.clone() });
    if (supported.is_err())
        return Err(
            Rejected<ExecutionRequest> { rstd::move(supported).unwrap_err(), rstd::move(request) });
    auto policy =
        options.policy.is_some() ? options.policy->clone() : state_->config.policy.clone();
    if (! valid_policy(policy, state_->config.cache.is_some()) ||
        (options.routing->route.is_some() && options.routing->shared_provider.is_some()) ||
        (options.cookies.is_Shared() && ! options.cookies.as_Shared().value))
        return Err(Rejected<ExecutionRequest> {
            Error::InvalidOptions(String::make("invalid HTTP policy or routing"_str)),
            rstd::move(request) });
    auto resolved = resolve_transfer_options(
        options.transfer, options.total_timeout, state_->config.total_timeout);
    if (resolved.is_err())
        return Err(
            Rejected<ExecutionRequest> { rstd::move(resolved).unwrap_err(), rstd::move(request) });
    options.transfer = *resolved;
    auto report =
        options.report.is_some() ? options.report->state() : Arc<ExecutionObserverState>::make();
    auto transfer_options = options.transfer;
    auto logical          = Arc<LogicalState>::make(state_.clone(),
                                                    rstd::move(request),
                                                    rstd::move(options),
                                                    rstd::move(policy),
                                                    report.clone());
    auto submitted        = state_->scope.submit(
        PreparedOperation<ResponseMessage>::make(LogicalDriver { logical.clone() }),
        transfer_options);
    if (submitted.is_err()) {
        auto rejected = rstd::move(submitted).unwrap_err();
        return Err(Rejected<ExecutionRequest> { rstd::move(rejected.error),
                                                logical->request.take().unwrap() });
    }
    auto transfer = rstd::move(submitted).unwrap();
    {
        auto f     = report->fields.lock().unwrap();
        f->control = Some(transfer.control());
    }
    return Ok(rstd::move(transfer));
}
void Client::request_close() const {
    stop_http_client(state_);
}
auto client_close_report(Arc<ClientState> state, coro<Result<empty>> closing) -> coro<CloseReport> {
    auto closed = co_await rstd::move(closing);
    auto f      = state->fields.lock().unwrap();
    if (f->close_report.is_some()) co_return f->close_report->clone();
    CloseReport report;
    if (closed.is_err()) report.add(rstd::move(closed).unwrap_err());
    co_return rstd::move(report);
}
auto Client::close() const -> coro<CloseReport> {
    request_close();
    return client_close_report(state_.clone(), registration_->release());
}
} // namespace lifetchto::http
