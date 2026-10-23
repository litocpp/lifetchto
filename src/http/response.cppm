export module lifetchto.http:response;
export import :error;
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
    bool          reading {}, finished {}, canceled {};
    Option<Error> error;
};
struct ResponseReadState {
    Arc<rstd::dyn<ResponseSource>>        source;
    rstd::sync::Mutex<ResponseReadFields> fields { ResponseReadFields {} };
    Event                                 changed;
    rstd::async::Mutex<empty>             close_gate { empty {} };
    Option<coro<Result<empty>>::Future>   close_task;
    Option<Error>                         close_error;
    bool                                  closed {};
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
    auto close() const -> coro<Result<empty>>;
};
export class ResponseBody {
    Arc<ResponseReadState> state_;

public:
    using Error = lifetchto::Error;

private:
    explicit ResponseBody(Arc<ResponseReadState> state): state_(rstd::move(state)) {}

public:
    static auto from_source(Arc<rstd::dyn<ResponseSource>> source) -> ResponseBody {
        return ResponseBody(Arc<ResponseReadState>::make(rstd::move(source)));
    }
    ResponseBody(ResponseBody&&) noexcept = default;
    ResponseBody(const ResponseBody&)     = delete;
    auto operator=(ResponseBody&& other) noexcept -> ResponseBody& {
        if (this != &other) {
            cancel();
            state_ = rstd::move(other.state_);
        }
        return *this;
    }
    ~ResponseBody() { cancel(); }
    auto next() -> coro<Result<Option<Bytes>>>;
    auto trailers() const -> Result<Option<lihttpto::Headers>>;
    auto completion() const -> BodyControl { return BodyControl(state_.clone()); }
    void cancel() const {
        if (state_) cancel_response(state_);
    }
    auto close() const -> coro<Result<empty>> { return completion().close(); }
};
export using ResponseMessage = lihttpto::ResponseMessage<ResponseBody>;
} // namespace lifetchto::http
