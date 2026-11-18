module lifetchto.http:driver;
import :state;

namespace lifetchto::http
{
auto driver(Arc<NativeState> client, Arc<Slot> slot) -> coro<rstd::empty>;
} // namespace lifetchto::http
