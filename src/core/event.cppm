export module lifetchto.core:event;
import rstd;
using namespace rstd::prelude;
namespace lifetchto
{
using rstd::sync::Arc;
using rstd::sync::Weak;
struct EventSlot {
    Option<rstd::task::Waker> waker;
};
struct EventFields {
    u64                  version {};
    Vec<Weak<EventSlot>> slots;
};
struct EventState {
    rstd::sync::Mutex<EventFields> fields { EventFields {} };
};
export class EventWait {
    Arc<EventState> state_;
    Arc<EventSlot>  slot_ { Arc<EventSlot>::make() };
    u64             version_;
    bool            registered_ {};

public:
    using Output = empty;
    EventWait(Arc<EventState> state, u64 version): state_(rstd::move(state)), version_(version) {}
    auto poll(mut_ref<EventWait> self, rstd::task::Context& cx) -> rstd::task::Poll<empty> {
        auto f = self->state_->fields.lock().unwrap();
        if (f->version != self->version_) return rstd::task::Poll<empty>::Ready(empty {});
        if (! self->registered_) {
            for (usize i; i < f->slots.len();) {
                if (! f->slots[i].upgrade())
                    f->slots.remove(i);
                else
                    ++i;
            }
            f->slots.push(self->slot_.downgrade());
            self->registered_ = true;
        }
        self->slot_->waker = Some(cx.waker().clone());
        return rstd::task::Poll<empty>::Pending();
    }
};
export class Event {
    Arc<EventState> state_ { Arc<EventState>::make() };

public:
    auto clone() const -> Event {
        Event e;
        e.state_ = state_.clone();
        return e;
    }
    auto listen() const -> EventWait {
        auto f = state_->fields.lock().unwrap();
        return EventWait(state_.clone(), f->version);
    }
    void notify() const {
        Vec<rstd::task::Waker> wake;
        {
            auto f = state_->fields.lock().unwrap();
            ++f->version;
            for (const auto& weak : f->slots) {
                auto slot = weak.upgrade();
                if (slot && slot->waker.is_some()) wake.push(slot->waker.take().unwrap());
            }
            f->slots.clear();
        }
        for (auto& w : wake) rstd::move(w).wake();
    }
};
} // namespace lifetchto
