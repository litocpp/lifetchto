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
    Option<lihttpto::ResponseHead> head;
    Option<ByteQueue>              queue;
    Option<lihttpto::Headers>      trailers;
    Option<Error>                  error;
    Option<Error>                  cleanup_error;
    bool reusable {}, canceled {}, finished {}, consumed {}, reading {}, upload_done {},
        upload_closed {}, stop_upload {}, continued {}, released {}, dispatched {},
        upload_pulled {};
    u64 sent_bytes {}, accepted_bytes {}, received_bytes {};
};
struct BodyState {
    rstd::sync::Mutex<BodyFields> fields { BodyFields {} };
    Event                         changed;
    Option<Instant>               deadline;
    explicit BodyState(Option<Instant> deadline): deadline(deadline) {}
    void fail(Error e) {
        {
            auto f = fields.lock().unwrap();
            if (f->error.is_none()) {
                if (f->queue.is_some()) f->queue->fail(e.clone());
                f->error = Some(rstd::move(e));
            }
        }
        changed.notify();
    }
    void cancel(StopReason reason = StopReason::Canceled) {
        {
            auto f      = fields.lock().unwrap();
            f->canceled = true;
            if (reason == StopReason::Deadline && f->error.is_none())
                f->error = Some(Error::Timeout());
            if (f->queue.is_some()) f->queue->fail(stop_error(reason));
        }
        changed.notify();
    }
};
struct Work {
    ExchangeRequest          request;
    lihttpto::RequestEncoder encoder;
    Arc<BodyState>           body;
};
struct SlotFields {
    Option<Work>           pending;
    Option<Arc<BodyState>> active;
    bool                   idle {}, closing {}, done {}, joined {};
};
struct Slot {
    String                        origin;
    Option<Arc<Route>>            route;
    rstd::sync::Mutex<SlotFields> fields { SlotFields {} };
    Event                         changed;
    Option<TaskControl>           task;
    Option<ResourcePermit>        permit;
    rstd::async::Mutex<empty>     join_gate { empty {} };
    Slot(String origin, Option<Arc<Route>> route)
        : origin(rstd::move(origin)), route(rstd::move(route)) {}
};
struct NativeFields {
    Vec<Arc<Slot>> slots;
    Vec<Arc<Slot>> retired;
    usize          waiters {}, handles { 1 };
    bool           closed {};
    Option<Error>  close_error;
};
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
auto body_stop(Arc<BodyState> body, bool writing = false) -> coro<Error> {
    for (;;) {
        auto event = body->changed.listen();
        {
            auto f = body->fields.lock().unwrap();
            if (f->error.is_some()) co_return f->error->clone();
            if (f->canceled || (writing && f->stop_upload)) co_return Error::Canceled();
        }
        if (body->deadline.is_some()) {
            auto ready = co_await rstd::async::select(
                rstd::move(event), rstd::async::sleep(remaining(*body->deadline)));
            if (ready.is_right()) co_return Error::Timeout();
        } else
            co_await rstd::move(event);
    }
}
auto connect_stop(Arc<BodyState> body, Instant deadline) -> coro<Error> {
    auto result = co_await rstd::async::select(body_stop(body.clone()).into_future(),
                                               rstd::async::sleep(remaining(deadline)));
    if (result.is_left()) co_return rstd::move(result).unwrap_left();
    co_return Error::Timeout();
}
template<class T>
auto guarded(Arc<BodyState> body, rstd::async::coro<Result<T>> task, bool writing = false)
    -> coro<Result<T>> {
    {
        auto f = body->fields.lock().unwrap();
        if (f->error.is_some()) co_return Err(f->error->clone());
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
