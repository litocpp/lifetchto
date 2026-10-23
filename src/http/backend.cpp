module lifetchto.http;
import :backend;
import :state;
import :driver;
import :native_body;
using namespace rstd::prelude;
using namespace rstd::literals;
namespace lifetchto::http
{
void stop_client(const Arc<NativeState>& state) {
    if (! state) return;
    {
        auto f     = state->fields.lock().unwrap();
        f->closed  = true;
        f->waiters = usize();
        for (const auto& slot : f->slots) {
            {
                auto s     = slot->fields.lock().unwrap();
                s->closing = true;
                if (s->active.is_some()) (*s->active)->cancel();
                if (s->pending.is_some()) s->pending->body->cancel();
            }
            slot->changed.notify();
        }
    }
    state->changed.notify();
}
void drop_client(const Arc<NativeState>& state) {
    if (! state) return;
    bool last;
    {
        auto f = state->fields.lock().unwrap();
        last   = --f->handles == usize();
    }
    if (last) stop_client(state);
}
struct QueueGuard {
    Arc<NativeState> state;
    bool             counted {};
    ~QueueGuard() {
        if (counted) {
            auto f = state->fields.lock().unwrap();
            if (! f->closed) --f->waiters;
        }
        state->changed.notify();
    }
};
auto same_route(const Option<Arc<Route>>& left, const Option<Arc<Route>>& right) -> bool {
    if (left.is_none() || right.is_none()) return left.is_none() && right.is_none();
    return Arc<Route>::ptr_eq(*left, *right);
}
auto join_slot(Arc<Slot> slot) -> coro<Result<empty>>;
struct AttemptData {
    Arc<NativeState>      native;
    Option<ExchangeInput> input;
    Arc<BodyState>        body { Arc<BodyState>::make(None<Instant>()) };
    AttemptData(Arc<NativeState> native, ExchangeInput input)
        : native(rstd::move(native)), input(Some(rstd::move(input))) {}
};
struct NativeSlotTask {
    rstd::sync::Weak<NativeState> state;
    rstd::sync::Weak<Slot>        slot;
    auto                          run(Cancellation) -> coro<Result<empty>> {
        auto owner      = state.upgrade();
        auto connection = slot.upgrade();
        if (owner && connection) co_await driver(rstd::move(owner), rstd::move(connection));
        co_return Ok(empty {});
    }
    void request_stop(StopReason) {
        auto connection = slot.upgrade();
        if (! connection) return;
        {
            auto s     = connection->fields.lock().unwrap();
            s->closing = true;
            if (s->active.is_some()) (*s->active)->cancel();
            if (s->pending.is_some()) s->pending->body->cancel();
        }
        connection->changed.notify();
    }
};
auto perform_exchange(Arc<AttemptData> attempt, TransferContext context)
    -> coro<Result<ResponseMessage>> {
    auto  state     = attempt->native.clone();
    auto& prepared  = attempt->input->prepared;
    auto  identity  = prepared.parts().url.http_origin()->serialize();
    auto  route     = prepared.target().route.clone();
    auto  resources = state->resources.resources();
    if (resources.is_err()) co_return Err(rstd::move(resources).unwrap_err());
    auto deadline  = context.deadline();
    auto body      = attempt->body.clone();
    body->deadline = deadline;
    CancelOnDrop      cancellation(body.clone());
    QueueGuard        queue { state.clone() };
    Option<Arc<Slot>> selected;
    bool              launch = false;
    Option<Arc<Slot>> retiring;
    for (;;) {
        auto event = state->changed.listen();
        {
            auto f = state->fields.lock().unwrap();
            if (f->closed || context.cancellation().is_canceled()) co_return Err(Error::Canceled());
            for (usize i; i < f->slots.len();) {
                bool done;
                {
                    auto s = f->slots[i]->fields.lock().unwrap();
                    done   = s->done;
                }
                if (done) {
                    auto slot = f->slots.remove(i);
                    f->retired.push(rstd::move(slot));
                } else
                    ++i;
            }
            for (usize i; i < f->retired.len();) {
                bool joined;
                {
                    auto s = f->retired[i]->fields.lock().unwrap();
                    joined = s->joined;
                }
                if (joined)
                    f->retired.remove(i);
                else
                    ++i;
            }
            usize same {};
            for (const auto& slot : f->retired)
                if (slot->origin == identity) ++same;
            bool returning = false;
            for (const auto& slot : f->slots) {
                if (slot->origin != identity) continue;
                ++same;
                auto s = slot->fields.lock().unwrap();
                if (! s->idle && ! s->closing && s->active.is_some()) {
                    auto b = (*s->active)->fields.lock().unwrap();
                    returning =
                        returning || (b->reusable && b->finished && b->consumed && b->upload_done &&
                                      b->upload_closed && ! b->canceled && b->error.is_none());
                }
                if (s->idle && ! s->closing && same_route(slot->route, route)) {
                    s->idle  = false;
                    selected = Some(slot.clone());
                    break;
                }
            }
            if (selected.is_none() && ! returning &&
                f->slots.len() + f->retired.len() < state->options.max_connections &&
                same < state->options.max_connections_per_origin) {
                auto quota = resources->try_reserve(ResourceAmount {
                    .resources = usize(1),
                    .bytes     = state->options.receive_window + state->options.upload_window +
                                 state->options.head_limit });
                auto slot =
                    Arc<Slot>::make(identity.clone(),
                                    route.is_some() ? Some((*route).clone()) : None<Arc<Route>>());
                if (quota.is_err()) co_return Err(rstd::move(quota).unwrap_err());
                slot->permit = Some(rstd::move(quota).unwrap());
                f->slots.push(slot.clone());
                selected = Some(rstd::move(slot));
                launch   = true;
            }
            if (selected.is_some()) {
                auto s         = (*selected)->fields.lock().unwrap();
                s->active      = Some(body.clone());
                s->pending     = Some(Work {
                    ExchangeRequest({ prepared.parts().clone(), rstd::move(attempt->input->body) }),
                    prepared.encoder(),
                    body.clone() });
                attempt->input = None();
                if (queue.counted) {
                    --f->waiters;
                    queue.counted = false;
                }
            } else {
                if (! f->retired.is_empty()) retiring = Some(f->retired[usize()].clone());
                for (const auto& slot : f->slots) {
                    auto s = slot->fields.lock().unwrap();
                    if (s->idle && ! s->closing) {
                        s->closing = true;
                        slot->changed.notify();
                        break;
                    }
                }
                if (! queue.counted) {
                    if (f->waiters >= state->options.max_waiters) co_return Err(Error::Capacity());
                    ++f->waiters;
                    queue.counted = true;
                }
            }
            if (launch) {
                auto task =
                    resources->spawn(NativeSlotTask { state.downgrade(), (*selected).downgrade() });
                if (task.is_err()) {
                    (*selected)->fields.lock().unwrap()->done = true;
                    body->fail(Error::Capacity());
                } else
                    (*selected)->task = Some(rstd::move(task).unwrap());
            }
        }
        if (selected.is_some()) break;
        if (retiring.is_some()) {
            auto          join   = join_slot(retiring.take().unwrap()).into_future();
            Result<empty> result = Ok(empty {});
            if (deadline.is_some()) {
                auto ready = co_await rstd::async::select(rstd::future::by_ref(join),
                                                          rstd::async::sleep(remaining(*deadline)));
                if (ready.is_right()) co_return Err(Error::Timeout());
                result = rstd::move(ready).unwrap_left();
            } else
                result = co_await rstd::move(join);
            state->changed.notify();
            if (result.is_err()) co_return Err(rstd::move(result).unwrap_err());
            continue;
        }
        if (deadline.is_some()) {
            auto waking = rstd::async::select(rstd::move(event),
                                              context.cancellation().stopped().into_future());
            auto ready  = co_await rstd::async::select(rstd::move(waking),
                                                       rstd::async::sleep(remaining(*deadline)));
            if (ready.is_right()) co_return Err(Error::Timeout());
        } else
            (void)co_await rstd::async::select(rstd::move(event),
                                               context.cancellation().stopped().into_future());
    }
    if ((*selected)->fields.lock().unwrap()->done) {
        Option<Work> pending;
        {
            auto f    = (*selected)->fields.lock().unwrap();
            pending   = f->pending.take();
            f->active = None();
        }
        if (pending.is_some()) {
            pending->request.body().cancel();
            auto closed = co_await pending->request.body().close();
            if (closed.is_err()) {
                auto f           = body->fields.lock().unwrap();
                f->cleanup_error = Some(rstd::move(closed).unwrap_err());
            }
        }
        {
            auto f      = body->fields.lock().unwrap();
            f->released = true;
        }
        (*selected)->permit = None();
        body->changed.notify();
        state->changed.notify();
        co_return Err(Error::Capacity());
    }
    (*selected)->changed.notify();
    for (;;) {
        auto event = body->changed.listen();
        {
            auto f = body->fields.lock().unwrap();
            if (f->head.is_some()) {
                cancellation.armed = false;
                co_return Ok(
                    ResponseMessage { f->head.take().unwrap(), native_body(body.clone()) });
            }
            if (f->error.is_some()) co_return Err(f->error->clone());
            if (f->canceled) co_return Err(Error::Canceled());
        }
        auto ready =
            co_await rstd::async::select(rstd::move(event), body_stop(body.clone()).into_future());
        if (ready.is_right()) co_return Err(rstd::move(ready).unwrap_right());
    }
}
auto join_slot(Arc<Slot> slot) -> coro<Result<empty>> {
    auto guard = co_await slot->join_gate.lock();
    if (slot->task.is_some()) {
        auto result = co_await slot->task->wait();
        slot->task  = None();
        {
            auto s    = slot->fields.lock().unwrap();
            s->joined = true;
        }
        if (result.is_err()) co_return Err(Error::Closed());
    }
    co_return Ok(empty {});
}
auto close_client(Arc<NativeState> state) -> coro<Result<empty>> {
    stop_client(state);
    Vec<Arc<Slot>> slots;
    {
        auto f = state->fields.lock().unwrap();
        for (const auto& slot : f->slots) slots.push(slot.clone());
        for (const auto& slot : f->retired) slots.push(slot.clone());
    }
    for (auto& slot : slots) {
        auto result = co_await join_slot(slot.clone());
        if (result.is_err()) {
            auto f = state->fields.lock().unwrap();
            if (f->close_error.is_none()) f->close_error = Some(rstd::move(result).unwrap_err());
        }
    }
    auto f = state->fields.lock().unwrap();
    if (f->close_error.is_some()) co_return Err(f->close_error->clone());
    co_return Ok(empty {});
}
} // namespace lifetchto::http
namespace lifetchto::http
{
struct NativeOwner {
    Arc<NativeState> state;
    void             request_stop(StopReason) const { stop_client(state); }
    auto             release(ReleaseContext) const -> coro<Result<empty>> {
        return close_client(state.clone());
    }
};
struct AttemptLease {
    Arc<BodyState> body;
    void           request_stop(StopReason) const {
        auto f = body->fields.lock().unwrap();
        if (! f->released) {
            f->canceled = true;
            body->changed.notify();
        }
    }
    auto release(ReleaseContext) const -> coro<Result<empty>> {
        (void)co_await wait_body_release(body.clone());
        co_return Ok(empty {});
    }
};
struct NativeAttempt {
    Arc<AttemptData> data;
    auto             run(TransferContext context, OutputSender<ResponseMessage> output)
        -> coro<ProtocolCompletion>;
    void request_stop(StopReason reason) {
        data->body->cancel(reason);
        data->native->changed.notify();
    }
    auto release(ReleaseContext) -> coro<Result<empty>> {
        if (data->input.is_some()) {
            data->input->body.cancel();
            auto closed = co_await data->input->body.close();
            {
                auto f      = data->body->fields.lock().unwrap();
                f->released = true;
                if (closed.is_err()) f->cleanup_error = Some(closed.as_ref().unwrap_err().clone());
            }
            data->body->changed.notify();
            data->input = None();
            {
                auto f = data->body->fields.lock().unwrap();
                if (f->queue.is_some()) f->queue->release();
            }
            co_return rstd::move(closed);
        }
        auto result = co_await wait_body_release(data->body.clone());
        {
            auto f = data->body->fields.lock().unwrap();
            if (f->queue.is_some()) f->queue->release();
        }
        co_return rstd::move(result);
    }
};
} // namespace lifetchto::http
namespace rstd
{
template<>
struct Impl<lifetchto::ManagedResource, lifetchto::http::NativeOwner>
    : ImplBase<lifetchto::http::NativeOwner> {
    void request_stop(lifetchto::StopReason reason) const { this->self().request_stop(reason); }
    auto release(lifetchto::ReleaseContext context) const { return this->self().release(context); }
};
template<>
struct Impl<lifetchto::ManagedResource, lifetchto::http::AttemptLease>
    : ImplBase<lifetchto::http::AttemptLease> {
    void request_stop(lifetchto::StopReason reason) const { this->self().request_stop(reason); }
    auto release(lifetchto::ReleaseContext context) const { return this->self().release(context); }
};
} // namespace rstd
namespace lifetchto::http
{
auto NativeAttempt::run(TransferContext context, OutputSender<ResponseMessage> output)
    -> coro<ProtocolCompletion> {
    auto allowance = context.begin_attempt();
    if (allowance.is_err())
        co_return ProtocolCompletion { Err(rstd::move(allowance).unwrap_err()),
                                       RemoteOutcome::NotDispatched };
    auto lease = context.resources().attach(
        empty {}, Arc<rstd::dyn<ManagedResource>>::make(AttemptLease { data->body.clone() }));
    if (lease.is_err()) {
        auto rejected = rstd::move(lease).unwrap_err();
        (void)co_await release({});
        rejected.input.request_close();
        (void)co_await rejected.input.close();
        co_return ProtocolCompletion { Err(rstd::move(rejected.error)), RemoteOutcome::Unknown };
    }
    auto owner = rstd::move(lease).unwrap();
    auto queue =
        co_await ByteQueue::make(context.resources(), data->native->options.receive_window);
    if (queue.is_err()) {
        (void)co_await release({});
        co_return ProtocolCompletion { Err(rstd::move(queue).unwrap_err()),
                                       RemoteOutcome::NotDispatched };
    }
    {
        auto f   = data->body->fields.lock().unwrap();
        f->queue = Some(rstd::move(queue).unwrap());
    }
    auto result = co_await perform_exchange(data.clone(), context.clone());
    if (data->input.is_some())
        co_return ProtocolCompletion {
            result.is_err()
                ? Result<empty>(Err(rstd::move(result).unwrap_err()))
                : Result<empty>(Err(Error::Contract(String::make("unsubmitted exchange"_str)))),
            RemoteOutcome::NotDispatched
        };
    if (result.is_ok()) {
        auto published = output.publish(rstd::move(result).unwrap());
        if (published.is_err()) data->body->cancel();
    } else
        data->body->cancel();
    (void)co_await wait_body_release(data->body.clone());
    auto f = data->body->fields.lock().unwrap();
    context.progress(Some(f->accepted_bytes),
                     f->dispatched ? None<u64>() : Some(u64()),
                     Some(f->received_bytes));
    if (f->error.is_some())
        co_return ProtocolCompletion { Err(f->error->clone()),
                                       f->head.is_some() || f->finished
                                           ? RemoteOutcome::Confirmed
                                           : (f->dispatched ? RemoteOutcome::Unknown
                                                            : RemoteOutcome::NotDispatched) };
    if (f->canceled && ! f->consumed)
        co_return ProtocolCompletion { Err(Error::Canceled()),
                                       f->dispatched ? RemoteOutcome::Unknown
                                                     : RemoteOutcome::NotDispatched };
    co_return ProtocolCompletion { Ok(empty {}), RemoteOutcome::Confirmed };
}
NativeBackend::NativeBackend(Arc<NativeState> state, Arc<ResourceLease<empty>> registration)
    : state_(rstd::move(state)), registration_(rstd::move(registration)) {
}
NativeBackend::NativeBackend(NativeBackend&&) noexcept = default;
NativeBackend::~NativeBackend() {
    drop_client(state_);
}
auto NativeBackend::operator=(NativeBackend&& other) noexcept -> NativeBackend& {
    if (this != &other) {
        drop_client(state_);
        state_        = rstd::move(other.state_);
        registration_ = rstd::move(other.registration_);
    }
    return *this;
}
auto NativeBackend::clone() const -> NativeBackend {
    {
        auto f = state_->fields.lock().unwrap();
        ++f->handles;
    }
    return NativeBackend(state_.clone(), registration_.clone());
}
auto NativeBackend::make(ResourceContext resources, TransportOptions options)
    -> Result<NativeBackend> {
    if (options.max_connections == usize() || options.max_connections_per_origin == usize() ||
        options.receive_window == usize() || options.upload_window == usize() ||
        options.head_limit < usize(16) || options.max_informational == usize() ||
        options.max_connections > usize(65536) || options.max_waiters > usize(65536))
        return Err(Error::InvalidOptions(String::make("invalid HTTP pool or buffer limits"_str)));
#if LITO_FEAT_TLS
    if (options.tls.is_none()) {
        auto profile = TlsProfile::make();
        if (profile.is_err()) return Err(rstd::move(profile).unwrap_err());
        options.tls = Some(rstd::move(profile).unwrap());
    }
#endif
    auto state    = Arc<NativeState>::make(resources.clone(), rstd::move(options));
    auto attached = resources.attach(
        empty {}, Arc<rstd::dyn<ManagedResource>>::make(NativeOwner { state.clone() }));
    if (attached.is_err()) return Err(rstd::move(attached).unwrap_err().error);
    return Ok(NativeBackend(rstd::move(state),
                            Arc<ResourceLease<empty>>::make(rstd::move(attached).unwrap())));
}
auto NativeBackend::prepare(lihttpto::RequestParts parts,
                            UploadMetadata         upload,
                            RouteChoice            route) const -> Result<PreparedExchange> {
    {
        auto f = state_->fields.lock().unwrap();
        if (f->closed) return Err(Error::Closed());
    }
    auto origin = parts.url.http_origin();
    if (origin.is_none())
        return Err(Error::InvalidOptions(String::make("HTTP origin required"_str)));
#if ! LITO_FEAT_TLS
    if (origin->scheme.as_str() == "https"_str)
        return Err(Error::Unsupported(String::make("HTTPS requires feature tls"_str)));
#endif
    if (route.route.is_some() && (*route.route)->origin() != origin->serialize().as_str())
        return Err(Error::InvalidOptions(String::make("route belongs to a different origin"_str)));
    auto prepared = lihttpto::PreparedRequest::make(
        rstd::move(parts),
        upload.length,
        state_->options.head_limit,
        upload.trailers.is_some() ? rstd::addressof(*upload.trailers) : nullptr);
    if (prepared.is_err())
        return Err(Error::InvalidOptions(String::make("invalid HTTP request metadata"_str)));
    auto identity = state_->identity.clone();
    auto cache    = CacheScope { identity.clone(), route.clone() };
    return Ok(PreparedExchange::make(rstd::move(prepared).unwrap(),
                                     rstd::move(identity),
                                     rstd::move(upload),
                                     rstd::move(route),
                                     rstd::move(cache)));
}
auto NativeBackend::operation(ExchangeInput input) const
    -> rstd::Result<PreparedOperation<ResponseMessage>, Rejected<ExchangeInput>> {
    if (! input.prepared.backend_identity().matches(state_->identity))
        return Err(Rejected<ExchangeInput> {
            Error::InvalidOptions(String::make("prepared exchange belongs to another backend"_str)),
            rstd::move(input) });
    if (input.body.length() != input.prepared.upload().length)
        return Err(Rejected<ExchangeInput> {
            Error::Contract(String::make("upload metadata mismatch"_str)), rstd::move(input) });
    return Ok(PreparedOperation<ResponseMessage>::make(
        NativeAttempt { Arc<AttemptData>::make(state_.clone(), rstd::move(input)) }));
}
auto NativeBackend::backend() const -> Arc<rstd::dyn<ExchangeBackend>> {
    return Arc<rstd::dyn<ExchangeBackend>>::make(clone());
}
void NativeBackend::request_close() const {
    stop_client(state_);
}
auto NativeBackend::close() const -> coro<Result<empty>> {
    request_close();
    return registration_->release();
}
} // namespace lifetchto::http
