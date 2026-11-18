module lifetchto.http:state;
import :backend;
import :event;
using namespace rstd::prelude;
namespace lifetchto::http
{
using rstd::sync::Arc;
using rstd::async::coro;
using rstd::bytes::Bytes;
struct BodyFields {
    ConnectStage                   connect_stage { ConnectStage::Dial };
    Option<lihttpto::ResponseHead> head;
    Option<ByteQueue>              queue;
    Option<lihttpto::Headers>      trailers;
    Option<Error>                  error;
    Option<FailureReport>          failure;
    Vec<Error>                     cleanup_errors;
    bool                           cleanup_taken {};
    Option<ResourcePermit>         metadata_permit;
    bool reusable {}, canceled {}, failed {}, finished {}, consumed {}, reading {}, upload_done {},
        upload_closed {}, stop_upload {}, continued {}, released {}, dispatched {},
        upload_pulled {};
    u64 sent_bytes {}, accepted_bytes {}, received_bytes {};
};
struct BodyState {
    rstd::sync::Mutex<BodyFields>    fields { BodyFields {} };
    Event                            changed;
    Option<Instant>                  deadline;
    Option<BodyTrafficObserver>      body_progress;
    rstd::sync::Mutex<Option<Event>> connection_changed { None<Event>() };
    void                             record_cleanup(Result<empty> result) const;
    void                             notify() const;
    explicit BodyState(Option<Instant> deadline, Option<BodyTrafficObserver> traffic = {})
        : deadline(deadline), body_progress(rstd::move(traffic)) {
        if (body_progress.is_some()) body_progress->initialize();
    }
    void set_connect_stage(ConnectStage stage) const;
    auto connect_stage() const -> ConnectStage { return fields.lock().unwrap()->connect_stage; }
    void sent_body(usize count) const {
        if (body_progress.is_some()) body_progress->sent(count);
    }
    void received_body(usize count) const {
        if (body_progress.is_some()) body_progress->received(count);
    }
    void fail(Error e);
    void cancel(StopReason reason = StopReason::Canceled);
};
struct Work {
    ExchangeRequest          request;
    lihttpto::RequestEncoder encoder;
    Arc<BodyState>           body;
#if LITO_FEAT_HTTP2
    Option<lihttpto::h2::Request> http2;
#endif
};
struct SlotFields {
    Option<Work>           pending;
    Option<Arc<BodyState>> active;
    bool                   idle {}, closing {}, done {}, joined {};
#if LITO_FEAT_HTTP2
    bool                http2 {};
    usize               http2_limit {}, http2_used {};
    Vec<Work>           http2_pending;
    Vec<Arc<BodyState>> http2_bodies;
#endif
};
struct Slot {
    String                        origin;
    Option<Arc<Route>>            route;
    Option<ProxyEndpoint>         proxy;
    rstd::sync::Mutex<SlotFields> fields { SlotFields {} };
    Event                         changed;
    Option<TaskControl>           task;
    Option<ResourcePermit>        permit;
    rstd::async::Mutex<empty>     join_gate { empty {} };
    Slot(String origin, Option<Arc<Route>> route, Option<ProxyEndpoint> proxy = {})
        : origin(rstd::move(origin)), route(rstd::move(route)), proxy(rstd::move(proxy)) {}
};
struct NativeDuplexRegistration {
    rstd::sync::Weak<ResourceLease<empty>> lease;
    Arc<rstd::sync::atomic::Atomic<bool>>  native_done;
};
struct NativeFields {
    Vec<Arc<Slot>>                slots;
    Vec<Arc<Slot>>                retired;
    Vec<NativeDuplexRegistration> duplex;
    usize                         waiters {}, handles { 1 };
    bool                          closed {};
    Vec<Error>                    close_errors;
};
auto active_duplex(NativeFields& fields) -> usize;
struct NativeState {
    rstd::async::RuntimeHandle      runtime;
    EngineHandle                    resources;
    BackendIdentity                 identity;
    TransportOptions                options;
    rstd::sync::Mutex<NativeFields> fields { NativeFields {} };
    Event                           changed;
    NativeState(ResourceContext resources, TransportOptions options)
        : runtime(resources.runtime()),
          resources(resources.handle()),
          options(rstd::move(options)) {}
};
auto body_stop(Arc<BodyState> body, bool writing = false) -> coro<Error>;
auto connect_stop(Arc<BodyState> body, Instant deadline) -> coro<Error>;
template<class T>
auto guarded(Arc<BodyState> body, rstd::async::coro<Result<T>> task, bool writing = false)
    -> coro<Result<T>> {
    {
        auto f = body->fields.lock().unwrap();
        if (f->failed) co_return Err(Error::Closed());
        if (f->canceled || (writing && f->stop_upload)) co_return Err(Error::Canceled());
    }
    if (body->deadline.is_some() && Instant::now() >= *body->deadline)
        co_return Err(Error::Timeout());
    auto work  = task.into_future();
    auto ready = co_await rstd::async::select(rstd::future::by_ref(work),
                                              body_stop(body.clone(), writing).into_future());
    if (ready.is_left()) co_return rstd::move(ready).unwrap_left();
    co_return Err(rstd::move(ready).unwrap_right());
}
struct CancelOnDrop {
    Arc<BodyState> body;
    bool           armed { true };
    explicit CancelOnDrop(Arc<BodyState> body): body(rstd::move(body)) {}
    ~CancelOnDrop() {
        if (armed && body) body->cancel();
    }
};
} // namespace lifetchto::http
