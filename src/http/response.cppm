export module lifetchto.http:response;
export import :cache_policy;
using namespace rstd::prelude;
namespace lifetchto::http
{
export struct ResponseSource {
    template<class Self, class = void>
    struct Api {
        using Trait = ResponseSource;
        auto next() const -> coro<Result<Option<Bytes>>> { return rstd::trait_call<0>(this); }
        auto trailers() const -> Result<Option<lihttpto::Headers>> {
            return rstd::trait_call<1>(this);
        }
        void cancel() const { rstd::trait_call<2>(this); }
        auto close() const -> coro<Result<empty>> { return rstd::trait_call<3>(this); }
    };
    template<class T>
    using Funcs = rstd::TraitFuncs<&T::next, &T::trailers, &T::cancel, &T::close>;
};
struct ResponseReadFields {
    bool                  reading {}, finished {}, canceled {};
    Option<FailureReport> failure;
    Option<StopReason>    stopped;
};
struct ResponseCleanupFields {
    Option<Result<empty>> result;
    CleanupReport         report;
    bool                  claimed {}, abandoned {};
};
struct ResponseReadState {
    Arc<rstd::dyn<ResponseSource>>           source;
    rstd::sync::Mutex<ResponseReadFields>    fields { ResponseReadFields {} };
    Event                                    changed;
    rstd::async::Mutex<empty>                close_gate { empty {} };
    Option<coro<Result<empty>>::Future>      close_task;
    rstd::sync::Mutex<ResponseCleanupFields> cleanup { ResponseCleanupFields {} };
    bool                                     closed {};
    explicit ResponseReadState(Arc<rstd::dyn<ResponseSource>> source): source(rstd::move(source)) {}
};
void cancel_response(const Arc<ResponseReadState>& state, StopReason reason = StopReason::Canceled);
export class BodyControl {
    Arc<ResponseReadState> state_;

public:
    explicit BodyControl(Arc<ResponseReadState> state): state_(rstd::move(state)) {}
    auto clone() const -> BodyControl { return BodyControl(state_.clone()); }
    void cancel(StopReason reason = StopReason::Canceled) const { cancel_response(state_, reason); }
    auto wait() const -> coro<Result<empty>>;
    auto failure() const -> Option<FailureReport>;
    auto close() const -> coro<CleanupReport>;
};
export class BodyCleanup {
    Arc<ResponseReadState> state_;

public:
    explicit BodyCleanup(Arc<ResponseReadState> state): state_(rstd::move(state)) {}
    BodyCleanup(BodyCleanup&&) noexcept = default;
    BodyCleanup(const BodyCleanup&)     = delete;
    ~BodyCleanup() {
        if (state_) {
            auto f       = state_->cleanup.lock().unwrap();
            f->abandoned = true;
            f->result    = None();
        }
    }
    void cancel(StopReason reason = StopReason::Canceled) const { cancel_response(state_, reason); }
    auto close() const -> coro<Result<empty>>;
};
export class ResponseBody {
    Arc<ResponseReadState> state_;

public:
    using Error = lifetchto::Error;

private:
    explicit ResponseBody(Arc<ResponseReadState> state): state_(rstd::move(state)) {}
    void abandon() const {
        if (! state_) return;
        cancel_response(state_);
        auto f = state_->cleanup.lock().unwrap();
        if (! f->claimed) {
            f->abandoned = true;
            f->result    = None();
        }
    }

public:
    static auto from_source(Arc<rstd::dyn<ResponseSource>> source) -> ResponseBody {
        return ResponseBody(Arc<ResponseReadState>::make(rstd::move(source)));
    }
    ResponseBody(ResponseBody&&) noexcept = default;
    ResponseBody(const ResponseBody&)     = delete;
    auto operator=(ResponseBody&& other) noexcept -> ResponseBody& {
        if (this != &other) {
            abandon();
            state_ = rstd::move(other.state_);
        }
        return *this;
    }
    ~ResponseBody() { abandon(); }
    auto next() -> coro<Result<Option<Bytes>>>;
    auto trailers() const -> Result<Option<lihttpto::Headers>>;
    auto completion() const -> BodyControl { return BodyControl(state_.clone()); }
    void cancel() const {
        if (state_) cancel_response(state_);
    }
    auto close() const -> coro<CleanupReport> { return completion().close(); }
    auto take_close_result() const -> Option<Result<empty>> {
        auto f = state_->cleanup.lock().unwrap();
        if (f->claimed) return None();
        return f->result.take();
    }
    auto take_cleanup() -> Result<BodyCleanup> {
        auto f = state_->cleanup.lock().unwrap();
        if (f->claimed) return Err(Error::Contract("body cleanup already claimed"_Str));
        f->claimed = true;
        return Ok(BodyCleanup(state_.clone()));
    }
};
export struct ResponseMessage {
    lihttpto::ResponseHead head;
    ResponseBody           body;
    CacheObservation       cache;
};
} // namespace lifetchto::http
