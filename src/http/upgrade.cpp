module lifetchto.http;
import :upgrade;
using namespace rstd::prelude;
using namespace rstd::literals;
namespace lifetchto::http
{
struct UpgradeConnectionFields {
    Bytes pending;
    bool  reading {};
};
struct UpgradeConnectionState {
    DuplexConnection                           wire;
    lihttpto::ResponseHead                     response;
    rstd::sync::Mutex<UpgradeConnectionFields> fields;
    UpgradeConnectionState(DuplexConnection wire, lihttpto::ResponseHead response, Bytes pending)
        : wire(rstd::move(wire)),
          response(rstd::move(response)),
          fields(UpgradeConnectionFields { rstd::move(pending) }) {}
};
UpgradeConnection::UpgradeConnection(DuplexConnection       wire,
                                     lihttpto::ResponseHead response,
                                     Bytes                  pending)
    : state_(Arc<UpgradeConnectionState>::make(rstd::move(wire),
                                               rstd::move(response),
                                               rstd::move(pending))) {
}
UpgradeConnection::UpgradeConnection(UpgradeConnection&&) noexcept = default;
auto UpgradeConnection::operator=(UpgradeConnection&& other) noexcept -> UpgradeConnection& {
    if (this != &other) {
        if (state_) state_->wire.cancel();
        state_ = rstd::move(other.state_);
    }
    return *this;
}
UpgradeConnection::~UpgradeConnection() {
    if (state_) state_->wire.cancel();
}
struct UpgradeReadGuard {
    Arc<UpgradeConnectionState> state;
    bool                        complete {};
    ~UpgradeReadGuard() {
        state->fields.lock().unwrap()->reading = false;
        if (! complete) state->wire.cancel();
    }
};
auto UpgradeConnection::read_from(Arc<UpgradeConnectionState> state) -> coro<Result<Bytes>> {
    {
        auto f = state->fields.lock().unwrap();
        if (f->reading) co_return Err(Error::Busy());
        if (! f->pending.is_empty()) co_return Ok(rstd::exchange(f->pending, Bytes {}));
        f->reading = true;
    }
    UpgradeReadGuard guard { state.clone() };
    auto             result = co_await state->wire.next();
    guard.complete          = true;
    if (result.is_err()) co_return Err(rstd::move(result).unwrap_err());
    co_return Ok(result->is_some() ? result->take().unwrap() : Bytes {});
}
auto UpgradeConnection::response() const -> const lihttpto::ResponseHead& {
    return state_->response;
}
auto UpgradeConnection::read() const -> coro<Result<Bytes>> {
    return read_from(state_.clone());
}
auto UpgradeConnection::write(Bytes bytes) const -> coro<Result<usize>> {
    return state_->wire.write(rstd::move(bytes));
}
auto UpgradeConnection::close_input() const -> coro<Result<empty>> {
    return state_->wire.close_input();
}
auto UpgradeConnection::can_close_input() const -> bool {
    return state_->wire.can_close_input();
}
auto UpgradeConnection::finish() const -> Result<empty> {
    return state_->wire.finish();
}
auto UpgradeConnection::completion() const -> TransferControl {
    return state_->wire.completion();
}
void UpgradeConnection::cancel() const {
    state_->wire.cancel();
}
auto UpgradeConnection::close() const -> coro<Arc<TransferReport>> {
    return state_->wire.close();
}
struct PendingUpgradeFields {
    Option<PendingDuplex>            pending;
    Option<lihttpto::UpgradeRequest> request;
    bool                             handed {}, abandoned {};
};
struct PendingUpgradeState {
    TransferControl                         control;
    UpgradeOptions                          options;
    Instant                                 started { Instant::now() };
    rstd::sync::Mutex<PendingUpgradeFields> fields;
    PendingUpgradeState(PendingDuplex            pending,
                        lihttpto::UpgradeRequest request,
                        UpgradeOptions           options)
        : control(pending.completion()),
          options(options),
          fields(PendingUpgradeFields { Some(rstd::move(pending)), Some(rstd::move(request)) }) {}
};
PendingUpgrade::PendingUpgrade(PendingDuplex            pending,
                               lihttpto::UpgradeRequest request,
                               UpgradeOptions           options)
    : state_(Arc<PendingUpgradeState>::make(rstd::move(pending), rstd::move(request), options)) {
}
PendingUpgrade::PendingUpgrade(PendingUpgrade&&) noexcept = default;
auto PendingUpgrade::operator=(PendingUpgrade&& other) noexcept -> PendingUpgrade& {
    if (this != &other) {
        abandon();
        state_ = rstd::move(other.state_);
    }
    return *this;
}
PendingUpgrade::~PendingUpgrade() {
    abandon();
}
void PendingUpgrade::abandon() const {
    if (! state_) return;
    auto f = state_->fields.lock().unwrap();
    if (! f->handed) {
        f->abandoned = true;
        state_->control.cancel();
    }
}
template<class T>
auto handshake_io(coro<Result<T>> operation, Instant deadline) -> coro<Result<T>> {
    if (Instant::now() >= deadline) co_return Err(Error::Timeout());
    auto result = co_await rstd::async::select(rstd::move(operation).into_future(),
                                               rstd::async::sleep(remaining(deadline)));
    if (result.is_right()) co_return Err(Error::Timeout());
    co_return rstd::move(result).unwrap_left();
}
auto handshake_read(const DuplexConnection& wire, Instant deadline) -> coro<Result<Bytes>> {
    auto result = co_await handshake_io(wire.next(), deadline);
    if (result.is_err()) co_return Err(rstd::move(result).unwrap_err());
    co_return Ok(result->is_some() ? result->take().unwrap() : Bytes {});
}
auto collect_upgrade_failure(const DuplexConnection&         wire,
                             const lihttpto::UpgradeRequest& request,
                             UpgradeFailure                  failure,
                             Bytes                           pending,
                             Instant                         deadline,
                             UpgradeOptions                  options) -> coro<UpgradeFailure> {
    auto framing = request.body_framing(*failure.response);
    if (framing.is_err()) {
        failure.body = Some(framing.unwrap_err());
        co_return rstd::move(failure);
    }
    lihttpto::BodyLimits limits;
    limits.data_bytes     = u64(options.error_body_bytes.to_primitive());
    limits.metadata_bytes = options.header_bytes;
    lihttpto::BodyDecoder decoder(*framing, limits);
    Vec<u8>               collected;
    for (;;) {
        auto progress = decoder.feed(pending.as_slice());
        if (progress.is_err()) {
            failure.body = Some(progress.unwrap_err());
            break;
        }
        collected.extend_from_slice(progress->data);
        pending.advance(progress->consumed);
        if (progress->status == lihttpto::DecodeStatus::Complete) {
            failure.body_complete = true;
            break;
        }
        if (! pending.is_empty()) continue;
        auto next = co_await handshake_read(wire, deadline);
        if (next.is_err()) {
            failure.transport = Some(rstd::move(next).unwrap_err());
            break;
        }
        pending = rstd::move(next).unwrap();
        if (pending.is_empty()) {
            auto finished = decoder.finish();
            if (finished.is_err())
                failure.body = Some(finished.unwrap_err());
            else
                failure.body_complete = true;
            break;
        }
    }
    failure.error_body = Bytes::copy_from_slice(collected.as_slice());
    co_return rstd::move(failure);
}
auto upgrade_handshake(DuplexConnection         wire,
                       lihttpto::UpgradeRequest request,
                       UpgradeOptions           options,
                       Instant deadline) -> coro<rstd::Result<UpgradeConnection, UpgradeFailure>> {
    auto sent = co_await handshake_io(wire.write(request.bytes().clone()), deadline);
    if (sent.is_err())
        co_return Err(UpgradeFailure { .transport = Some(rstd::move(sent).unwrap_err()) });
    lihttpto::Http1HeadParser parser(false, options.header_bytes);
    Bytes                     bytes;
    usize                     used, informational;
    for (;;) {
        if (bytes.is_empty()) {
            auto received = co_await handshake_read(wire, deadline);
            if (received.is_err())
                co_return Err(
                    UpgradeFailure { .transport = Some(rstd::move(received).unwrap_err()) });
            bytes = rstd::move(received).unwrap();
            if (bytes.is_empty())
                co_return Err(UpgradeFailure {
                    .transport = Some(protocol_error("truncated Upgrade response"_Str)) });
        }
        auto parsed = parser.push(bytes.as_slice());
        if (parsed.is_err())
            co_return Err(UpgradeFailure { .parse = Some(rstd::move(parsed).unwrap_err()) });
        if (parsed->is_NeedMore()) {
            bytes = {};
            continue;
        }
        auto completed = rstd::move(parsed).unwrap().as_Complete();
        auto head      = rstd::move(completed.head).into_response();
        if (head.is_err())
            co_return Err(UpgradeFailure { .parse = Some(rstd::move(head).unwrap_err()) });
        auto response = rstd::move(head).unwrap();
        bytes.advance(completed.input_consumed);
        auto disposition = request.classify(response);
        if (disposition.is_err())
            co_return Err(UpgradeFailure { .protocol = Some(disposition.unwrap_err()),
                                           .response = Some(rstd::move(response)) });
        if (*disposition == lihttpto::UpgradeDisposition::Informational) {
            used += completed.consumed;
            if (++informational > usize(8) || used >= options.header_bytes)
                co_return Err(
                    UpgradeFailure { .protocol = Some(lihttpto::UpgradeError::TooLarge) });
            parser = lihttpto::Http1HeadParser(false, options.header_bytes - used);
            continue;
        }
        if (*disposition == lihttpto::UpgradeDisposition::Rejected) {
            co_return Err(co_await collect_upgrade_failure(
                wire,
                request,
                UpgradeFailure { .protocol = Some(lihttpto::UpgradeError::RejectedStatus),
                                 .response = Some(rstd::move(response)) },
                rstd::move(bytes),
                deadline,
                options));
        }
        co_return Ok(UpgradeConnection(rstd::move(wire), rstd::move(response), rstd::move(bytes)));
    }
}
auto PendingUpgrade::receive(Arc<PendingUpgradeState> state)
    -> coro<rstd::Result<UpgradeConnection, UpgradeFailure>> {
    Option<PendingDuplex>            pending;
    Option<lihttpto::UpgradeRequest> request;
    {
        auto f = state->fields.lock().unwrap();
        if (f->pending.is_none() || f->abandoned)
            co_return Err(UpgradeFailure {
                .transport = Some(f->abandoned ? Error::Closed() : Error::OutputTaken()) });
        pending = f->pending.take();
        request = f->request.take();
    }
    auto deadline  = state->started + state->options.handshake_timeout;
    auto connected = co_await handshake_io(pending->connected(), deadline);
    if (connected.is_err()) {
        auto report = co_await state->control.close();
        co_return Err(UpgradeFailure { .transport = Some(rstd::move(connected).unwrap_err()),
                                       .report    = Some(rstd::move(report)) });
    }
    auto result = co_await upgrade_handshake(
        rstd::move(connected).unwrap(), request.take().unwrap(), state->options, deadline);
    if (result.is_err()) {
        auto failure   = rstd::move(result).unwrap_err();
        failure.report = Some(co_await state->control.close());
        co_return Err(rstd::move(failure));
    }
    auto f = state->fields.lock().unwrap();
    if (f->abandoned) co_return Err(UpgradeFailure { .transport = Some(Error::Closed()) });
    f->handed = true;
    co_return rstd::move(result);
}
auto PendingUpgrade::opened() const -> coro<rstd::Result<UpgradeConnection, UpgradeFailure>> {
    return receive(state_.clone());
}
auto PendingUpgrade::completion() const -> TransferControl {
    return state_->control.clone();
}
void PendingUpgrade::cancel() const {
    state_->control.cancel();
}
auto PendingUpgrade::close() const -> coro<Arc<TransferReport>> {
    return state_->control.close();
}
} // namespace lifetchto::http
