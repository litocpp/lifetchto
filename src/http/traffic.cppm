export module lifetchto.http:traffic;
import rstd;
using namespace rstd::prelude;
namespace lifetchto::http
{
using rstd::sync::Arc;
struct BodyState;
} // namespace lifetchto::http
export namespace lifetchto::http
{
struct BodyTraffic {
    Option<u64> body_sent_bytes;
    Option<u64> body_received_bytes;
};
class BodyTrafficObserver {
    Arc<rstd::sync::Mutex<BodyTraffic>> state_ { Arc<rstd::sync::Mutex<BodyTraffic>>::make(
        BodyTraffic {}) };
    explicit BodyTrafficObserver(Arc<rstd::sync::Mutex<BodyTraffic>> state)
        : state_(rstd::move(state)) {}
    friend struct BodyState;
    void initialize() const {
        auto f = state_->lock().unwrap();
        if (f->body_sent_bytes.is_none()) f->body_sent_bytes = Some(u64());
        if (f->body_received_bytes.is_none()) f->body_received_bytes = Some(u64());
    }
    void sent(usize count) const {
        auto f             = state_->lock().unwrap();
        f->body_sent_bytes = Some(f->body_sent_bytes->saturating_add(u64(count.to_primitive())));
    }
    void received(usize count) const {
        auto f = state_->lock().unwrap();
        f->body_received_bytes =
            Some(f->body_received_bytes->saturating_add(u64(count.to_primitive())));
    }

public:
    BodyTrafficObserver() = default;
    auto clone() const -> BodyTrafficObserver { return BodyTrafficObserver(state_.clone()); }
    // Counts confirmed plaintext body writes and decoded body queued for reading.
    // Canceled I/O can transfer additional bytes without a completion receipt.
    auto snapshot() const -> BodyTraffic { return *state_->lock().unwrap(); }
};
} // namespace lifetchto::http
