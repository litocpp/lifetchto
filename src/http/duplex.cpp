module lifetchto.http;
import :duplex;
using namespace rstd::prelude;
namespace lifetchto::http
{
struct PendingDuplexFields {
    Option<Transfer<DuplexStream>> transfer;
    bool                           reading {}, abandoned {};
};
struct PendingDuplexState {
    TransferControl                        control;
    rstd::sync::Mutex<PendingDuplexFields> fields;
    explicit PendingDuplexState(Transfer<DuplexStream> transfer)
        : control(transfer.control()), fields(PendingDuplexFields { Some(rstd::move(transfer)) }) {}
};
PendingDuplex::PendingDuplex(Transfer<DuplexStream> transfer)
    : state_(Arc<PendingDuplexState>::make(rstd::move(transfer))) {
}
PendingDuplex::PendingDuplex(PendingDuplex&&) noexcept = default;
auto PendingDuplex::operator=(PendingDuplex&& other) noexcept -> PendingDuplex& {
    if (this != &other) {
        abandon();
        state_ = rstd::move(other.state_);
    }
    return *this;
}
PendingDuplex::~PendingDuplex() {
    abandon();
}
void PendingDuplex::abandon() const {
    if (! state_) return;
    auto f = state_->fields.lock().unwrap();
    if (f->transfer.is_some()) {
        f->abandoned = true;
        state_->control.cancel();
    }
}
struct DuplexReceiveGuard {
    Arc<PendingDuplexState> state;
    bool                    complete {};
    ~DuplexReceiveGuard() {
        state->fields.lock().unwrap()->reading = false;
        if (! complete) state->control.cancel();
    }
};
auto PendingDuplex::receive(Arc<PendingDuplexState> state) -> coro<Result<DuplexConnection>> {
    Option<coro<Result<DuplexStream>>> output;
    {
        auto f = state->fields.lock().unwrap();
        if (f->abandoned) co_return Err(Error::Closed());
        if (f->transfer.is_none()) co_return Err(Error::OutputTaken());
        if (f->reading) co_return Err(Error::Busy());
        f->reading = true;
        output     = Some(f->transfer->output());
    }
    DuplexReceiveGuard guard { state.clone() };
    auto               stream = co_await output.take().unwrap();
    guard.complete            = true;
    if (stream.is_err()) co_return Err(rstd::move(stream).unwrap_err());
    auto f = state->fields.lock().unwrap();
    if (f->abandoned) co_return Err(Error::Closed());
    co_return Ok(DuplexConnection(f->transfer.take().unwrap(), rstd::move(stream).unwrap()));
}
auto PendingDuplex::connected() const -> coro<Result<DuplexConnection>> {
    return receive(state_.clone());
}
auto PendingDuplex::completion() const -> TransferControl {
    return state_->control.clone();
}
void PendingDuplex::cancel() const {
    state_->control.cancel();
}
auto PendingDuplex::close() const -> coro<Arc<TransferReport>> {
    return state_->control.close();
}
} // namespace lifetchto::http
