export module lifetchto.core:duplex;
export import :bytes;
using namespace rstd::prelude;
using namespace rstd::literals;
namespace lifetchto
{
struct DuplexState;
struct DuplexAckFields {
    Option<Result<usize>> result;
    bool                  abandoned {};
};
struct DuplexAck {
    rstd::sync::Mutex<DuplexAckFields> fields { DuplexAckFields {} };
    Event                              changed;
};
export class DuplexWrite {
    Arc<DuplexAck>                ack_;
    rstd::sync::Weak<DuplexState> state_;
    Bytes                         bytes_;
    bool                          half_close_ {}, completed_ {};
    friend class DuplexChannel;
    friend auto duplex_write(Arc<DuplexState>, Bytes, bool) -> coro<Result<usize>>;
    DuplexWrite(Arc<DuplexAck>                ack,
                rstd::sync::Weak<DuplexState> state,
                Bytes                         bytes,
                bool                          half_close)
        : ack_(rstd::move(ack)),
          state_(rstd::move(state)),
          bytes_(rstd::move(bytes)),
          half_close_(half_close) {}

public:
    DuplexWrite(DuplexWrite&& other) noexcept
        : ack_(rstd::move(other.ack_)),
          state_(rstd::move(other.state_)),
          bytes_(rstd::move(other.bytes_)),
          half_close_(other.half_close_),
          completed_(rstd::exchange(other.completed_, true)) {}
    DuplexWrite(const DuplexWrite&) = delete;
    auto operator=(DuplexWrite&& other) noexcept -> DuplexWrite& {
        if (this != &other) {
            if (! completed_) complete(Err(Error::Canceled()));
            ack_        = rstd::move(other.ack_);
            state_      = rstd::move(other.state_);
            bytes_      = rstd::move(other.bytes_);
            half_close_ = other.half_close_;
            completed_  = rstd::exchange(other.completed_, true);
        }
        return *this;
    }
    ~DuplexWrite();
    auto bytes() const -> const Bytes& { return bytes_; }
    auto is_half_close() const -> bool { return half_close_; }
    void complete(Result<usize> result);
};
struct DuplexFields {
    Option<DuplexWrite>   pending;
    Option<Result<empty>> terminal;
    Option<FailureReport> failure;
    Option<Error>         write_error;
    bool                  owner_dropped {};
    bool                  reading {}, writing {}, output_closed {}, released {}, taken {};
};
struct DuplexState {
    ByteQueue                       incoming;
    usize                           window;
    bool                            half_close;
    TransferControl                 control;
    Cancellation                    stopped;
    Event                           changed;
    rstd::sync::Mutex<DuplexFields> fields { DuplexFields {} };
    Option<ResourcePermit>          outgoing_permit;
    DuplexState(ByteQueue       incoming,
                usize           window,
                TransferControl control,
                ResourcePermit  permit,
                bool            half_close)
        : incoming(rstd::move(incoming)),
          window(window),
          half_close(half_close),
          control(rstd::move(control)),
          outgoing_permit(Some(rstd::move(permit))) {}
    void stop(StopReason reason) const;
    void fail(Error error, StopReason reason = StopReason::Canceled) const;
    void observe_failure(const Error& error, StopReason reason = StopReason::Canceled) const;
};
export class DuplexStream {
    Arc<DuplexState> state_;
    friend class DuplexChannel;
    explicit DuplexStream(Arc<DuplexState> state): state_(rstd::move(state)) {}
    void abandon() const;

public:
    DuplexStream(DuplexStream&&) noexcept = default;
    DuplexStream(const DuplexStream&)     = delete;
    auto operator=(DuplexStream&& other) noexcept -> DuplexStream& {
        if (this != &other) {
            abandon();
            state_ = rstd::move(other.state_);
        }
        return *this;
    }
    ~DuplexStream() { abandon(); }
    auto next() const -> coro<Result<Option<Bytes>>>;
    auto write(Bytes bytes) const -> coro<Result<usize>>;
    auto close_input() const -> coro<Result<empty>>;
    auto can_close_input() const -> bool { return state_->half_close; }
    auto finish() const -> Result<empty>;
    auto take_write_error() const -> Option<Error> {
        return state_->fields.lock().unwrap()->write_error.take();
    }
    auto completion() const -> TransferControl { return state_->control.clone(); }
    void cancel() const { state_->control.cancel(); }
    auto close() const -> coro<Arc<TransferReport>> { return state_->control.close(); }
};
export class DuplexChannel {
    Arc<DuplexState> state_;
    explicit DuplexChannel(Arc<DuplexState> state): state_(rstd::move(state)) {}

public:
    static auto make(TransferContext context, usize window = usize(65536), bool half_close = true)
        -> coro<Result<DuplexChannel>>;
    auto clone() const -> DuplexChannel { return DuplexChannel(state_.clone()); }
    auto take_stream() const -> Result<DuplexStream>;
    auto available() const -> usize { return state_->incoming.available(); }
    auto receive(Bytes bytes) const -> coro<Result<empty>> {
        return state_->incoming.send(rstd::move(bytes));
    }
    auto try_receive(Bytes bytes) const -> Result<empty> {
        return state_->incoming.try_send(rstd::move(bytes));
    }
    void end_input() const { state_->incoming.finish(); }
    auto next_write() const -> coro<Result<DuplexWrite>>;
    auto try_write() const -> Result<Option<DuplexWrite>>;
    void request_stop(StopReason reason = StopReason::Canceled) const { state_->stop(reason); }
    void fail(Error error) const { state_->fail(rstd::move(error)); }
    auto cancellation() const -> Cancellation { return state_->stopped.clone(); }
    auto wait_finished() const -> coro<Result<empty>>;
    auto failure() const -> Option<FailureReport> {
        auto f = state_->fields.lock().unwrap();
        return f->failure.is_some() ? Some(f->failure->clone()) : None<FailureReport>();
    }
    void release_buffers() const;
};
void DuplexState::observe_failure(const Error& error, StopReason reason) const {
    {
        auto f = fields.lock().unwrap();
        if (f->terminal.is_none()) {
            f->terminal = Some(Result<empty>(Err(stop_error(reason))));
            f->failure  = Some(report_failure(error));
        }
    }
    stopped.cancel(reason);
    changed.notify();
}
void DuplexState::fail(Error error, StopReason reason) const {
    observe_failure(error, reason);
    incoming.fail(rstd::move(error));
}
void DuplexState::stop(StopReason reason) const {
    fail(stop_error(reason), reason);
}
void DuplexWrite::complete(Result<usize> result) {
    if (completed_) return;
    completed_ = true;
    auto state = state_.upgrade();
    if (state && result.is_err() && ! result.as_ref().unwrap_err().is_Unsupported()) {
        state->observe_failure(result.as_ref().unwrap_err());
        state->incoming.fail(Error::Closed());
    }
    if (state && result.is_ok() && half_close_) state->fields.lock().unwrap()->output_closed = true;
    {
        auto f = ack_->fields.lock().unwrap();
        if (! f->abandoned)
            f->result = Some(rstd::move(result));
        else if (state && result.is_err()) {
            auto fields = state->fields.lock().unwrap();
            if (! fields->owner_dropped && fields->write_error.is_none())
                fields->write_error = Some(rstd::move(result).unwrap_err());
        }
    }
    ack_->changed.notify();
}
DuplexWrite::~DuplexWrite() {
    if (! completed_) complete(Err(Error::Canceled()));
}
void DuplexStream::abandon() const {
    if (! state_) return;
    bool finished;
    {
        auto f           = state_->fields.lock().unwrap();
        finished         = f->terminal.is_some() && f->terminal->is_ok();
        f->owner_dropped = true;
        f->write_error   = None();
    }
    if (! finished) state_->control.cancel();
}
struct DuplexReadGuard {
    Arc<DuplexState> state;
    bool             complete {};
    ~DuplexReadGuard() {
        state->fields.lock().unwrap()->reading = false;
        if (! complete) state->control.cancel();
    }
};
auto duplex_read(Arc<DuplexState> state) -> coro<Result<Option<Bytes>>> {
    {
        auto f = state->fields.lock().unwrap();
        if (f->released) co_return Err(Error::Closed());
        if (f->reading) co_return Err(Error::Busy());
        f->reading = true;
    }
    DuplexReadGuard guard { state.clone() };
    auto            result = co_await state->incoming.next(state->window);
    if (result.is_err()) state->observe_failure(result.as_ref().unwrap_err());
    guard.complete = true;
    co_return rstd::move(result);
}
auto DuplexStream::next() const -> coro<Result<Option<Bytes>>> {
    return duplex_read(state_.clone());
}
struct DuplexWriteGuard {
    Arc<DuplexState> state;
    Arc<DuplexAck>   ack;
    bool             complete {};
    ~DuplexWriteGuard() {
        if (! complete) {
            auto f       = ack->fields.lock().unwrap();
            f->abandoned = true;
            if (f->result.is_some() && f->result->is_err()) {
                auto fields = state->fields.lock().unwrap();
                if (! fields->owner_dropped && fields->write_error.is_none())
                    fields->write_error = Some(rstd::move(*f->result).unwrap_err());
                f->result = None();
            }
        }
        state->fields.lock().unwrap()->writing = false;
        if (! complete) state->control.cancel();
    }
};
auto duplex_write(Arc<DuplexState> state, Bytes bytes, bool half_close) -> coro<Result<usize>> {
    if (bytes.len() > state->window) co_return Err(Error::TooLarge());
    auto ack = Arc<DuplexAck>::make();
    {
        auto f = state->fields.lock().unwrap();
        if (f->released || f->terminal.is_some()) co_return Err(Error::Closed());
        if (f->writing) co_return Err(Error::Busy());
        if (f->output_closed)
            co_return half_close ? Result<usize>(Ok(usize())) : Result<usize>(Err(Error::Closed()));
        f->writing = true;
        f->pending =
            Some(DuplexWrite(ack.clone(), state.downgrade(), rstd::move(bytes), half_close));
    }
    DuplexWriteGuard guard { state.clone(), ack.clone() };
    state->changed.notify();
    for (;;) {
        auto event = ack->changed.listen();
        {
            auto f = ack->fields.lock().unwrap();
            if (f->result.is_some()) {
                guard.complete = true;
                co_return f->result.take().unwrap();
            }
        }
        auto ready =
            co_await rstd::async::select(rstd::move(event), state->stopped.stopped().into_future());
        if (ready.is_right()) {
            auto acked = ack->fields.lock().unwrap();
            if (acked->result.is_some()) {
                guard.complete = true;
                co_return acked->result.take().unwrap();
            }
            co_return Err(stop_error(rstd::move(ready).unwrap_right()));
        }
    }
}
auto DuplexStream::write(Bytes bytes) const -> coro<Result<usize>> {
    return duplex_write(state_.clone(), rstd::move(bytes), false);
}
auto duplex_half_close(Arc<DuplexState> state) -> coro<Result<empty>> {
    if (! state->half_close)
        co_return Err(Error::Unsupported("transport does not support send half-close"_Str));
    auto result = co_await duplex_write(rstd::move(state), {}, true);
    if (result.is_err()) co_return Err(rstd::move(result).unwrap_err());
    co_return Ok(empty {});
}
auto DuplexStream::close_input() const -> coro<Result<empty>> {
    return duplex_half_close(state_.clone());
}
auto DuplexStream::finish() const -> Result<empty> {
    {
        auto f = state_->fields.lock().unwrap();
        if (f->terminal.is_some())
            return f->terminal->is_ok()
                       ? Result<empty>(Ok(empty {}))
                       : Result<empty>(Err(stop_error(
                             state_->stopped.reason().unwrap_or(StopReason::OwnerClosed))));
        if (f->reading || f->writing) return Err(Error::Busy());
        f->terminal = Some(Result<empty>(Ok(empty {})));
    }
    state_->changed.notify();
    return Ok(empty {});
}
auto DuplexChannel::make(TransferContext context, usize window, bool half_close)
    -> coro<Result<DuplexChannel>> {
    auto incoming = co_await ByteQueue::make(context.resources(), window);
    if (incoming.is_err()) co_return Err(rstd::move(incoming).unwrap_err());
    auto permit = co_await context.resources().reserve(ResourceAmount { .bytes = window });
    if (permit.is_err()) co_return Err(rstd::move(permit).unwrap_err());
    co_return Ok(DuplexChannel(Arc<DuplexState>::make(rstd::move(incoming).unwrap(),
                                                      window,
                                                      context.control(),
                                                      rstd::move(permit).unwrap(),
                                                      half_close)));
}
auto DuplexChannel::take_stream() const -> Result<DuplexStream> {
    auto f = state_->fields.lock().unwrap();
    if (f->taken) return Err(Error::OutputTaken());
    f->taken = true;
    return Ok(DuplexStream(state_.clone()));
}
auto DuplexChannel::try_write() const -> Result<Option<DuplexWrite>> {
    auto f = state_->fields.lock().unwrap();
    if (state_->stopped.is_canceled()) return Err(stop_error(*state_->stopped.reason()));
    return Ok(f->pending.take());
}
auto duplex_next_write(DuplexChannel channel, Arc<DuplexState> state) -> coro<Result<DuplexWrite>> {
    for (;;) {
        auto event   = state->changed.listen();
        auto command = channel.try_write();
        if (command.is_err()) co_return Err(rstd::move(command).unwrap_err());
        if (command->is_some()) co_return Ok(command->take().unwrap());
        co_await rstd::move(event);
    }
}
auto DuplexChannel::next_write() const -> coro<Result<DuplexWrite>> {
    return duplex_next_write(clone(), state_.clone());
}
auto duplex_finished(Arc<DuplexState> state) -> coro<Result<empty>> {
    for (;;) {
        auto event = state->changed.listen();
        {
            auto f = state->fields.lock().unwrap();
            if (f->terminal.is_some())
                co_return f->terminal->is_ok()
                    ? Result<empty>(Ok(empty {}))
                    : Result<empty>(Err(
                          stop_error(state->stopped.reason().unwrap_or(StopReason::OwnerClosed))));
        }
        co_await rstd::move(event);
    }
}
auto DuplexChannel::wait_finished() const -> coro<Result<empty>> {
    return duplex_finished(state_.clone());
}
void DuplexChannel::release_buffers() const {
    state_->stop(StopReason::OwnerClosed);
    Option<DuplexWrite> pending;
    {
        auto f      = state_->fields.lock().unwrap();
        f->released = true;
        pending     = f->pending.take();
    }
    pending = None();
    state_->incoming.release();
    state_->outgoing_permit = None();
}
} // namespace lifetchto
