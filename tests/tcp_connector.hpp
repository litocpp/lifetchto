#pragma once
namespace test_connector
{
using namespace rstd::prelude;
using namespace rstd::literals;
using lifetchto::Arc;
using lifetchto::Duration;
using lifetchto::coro;
using rstd::net::SocketAddr;
struct Counts {
    usize prepared {}, started {}, completed {}, active {}, taken {}, closes {}, released {},
        closing_active {};
    Option<SocketAddr> destination;
};
using Stats = Arc<rstd::sync::Mutex<Counts>>;
struct FinishGuard {
    Stats stats;
    explicit FinishGuard(Stats stats): stats(rstd::move(stats)) {
        auto f = this->stats->lock().unwrap();
        ++f->started;
        ++f->active;
    }
    ~FinishGuard() {
        auto f = stats->lock().unwrap();
        --f->active;
    }
};
struct Dial {
    Arc<rstd::sync::Mutex<Option<rstd::net::TcpConnect>>> connecting;
    Stats                                                 stats;
    Duration                                              delay, cleanup;
    bool fail_finish {}, fail_take {}, fail_close {};
    auto finish() const -> coro<lifetchto::Result<empty>> {
        FinishGuard guard(stats.clone());
        co_await rstd::async::sleep(delay);
        if (fail_finish) co_return Err(lifetchto::Error::Unsupported("dial finish failed"_Str));
        auto operation = [&] {
            auto f = connecting->lock().unwrap();
            return (*f)->finish();
        }();
        auto result = co_await rstd::move(operation);
        if (result.is_err()) co_return Err(lifetchto::Error::Connect(result.unwrap_err()));
        {
            auto f = stats->lock().unwrap();
            ++f->completed;
        }
        co_return Ok(empty {});
    }
    auto into_stream() const -> lifetchto::Result<rstd::async::io::Stream> {
        if (fail_take) return Err(lifetchto::Error::Unsupported("dial handoff failed"_Str));
        auto pending = connecting->lock().unwrap()->take().unwrap();
        auto result  = rstd::move(pending).into_stream();
        if (result.is_err()) return Err(lifetchto::Error::Connect(result.unwrap_err()));
        {
            auto f = stats->lock().unwrap();
            ++f->taken;
        }
        auto stream = rstd::net::as_stream(rstd::move(*result));
        if (stream.is_err()) return Err(lifetchto::Error::Connect(stream.unwrap_err()));
        return Ok(rstd::move(*stream));
    }
    auto close() const -> coro<lifetchto::Result<empty>> {
        {
            auto f = stats->lock().unwrap();
            ++f->closes;
            f->closing_active += f->active;
        }
        auto pending = connecting->lock().unwrap()->take();
        co_await rstd::async::sleep(cleanup);
        if (pending.is_some()) (void)co_await rstd::move(pending.take().unwrap()).close();
        {
            auto f = stats->lock().unwrap();
            ++f->released;
        }
        if (fail_close) co_return Err(lifetchto::Error::Unsupported("dial close failed"_Str));
        co_return Ok(empty {});
    }
};
struct Connector {
    SocketAddr physical;
    Stats      stats;
    Duration   delay, cleanup;
    bool       fail_finish {}, fail_take {}, fail_close {};
    auto       prepare(SocketAddr destination) const
        -> lifetchto::Result<Arc<rstd::dyn<lifetchto::http::TcpDial>>>;
};
struct Endpoint {
    Connector connector;
    auto      prepare() const -> lifetchto::Result<Arc<rstd::dyn<lifetchto::http::TcpDial>>> {
        return connector.prepare(connector.physical);
    }
};
} // namespace test_connector
namespace rstd
{
template<>
struct Impl<lifetchto::http::TcpDial, test_connector::Dial> : ImplBase<test_connector::Dial> {
    auto finish() const { return this->self().finish(); }
    auto into_stream() const { return this->self().into_stream(); }
    auto close() const { return this->self().close(); }
};
template<>
struct Impl<lifetchto::http::TcpConnector, test_connector::Connector>
    : ImplBase<test_connector::Connector> {
    auto prepare(rstd::net::SocketAddr destination) const {
        return this->self().prepare(destination);
    }
};
template<>
struct Impl<lifetchto::http::EndpointConnector, test_connector::Endpoint>
    : ImplBase<test_connector::Endpoint> {
    auto prepare() const { return this->self().prepare(); }
};
} // namespace rstd
namespace test_connector
{
inline auto Connector::prepare(SocketAddr destination) const
    -> lifetchto::Result<Arc<rstd::dyn<lifetchto::http::TcpDial>>> {
    auto prepared = rstd::net::TcpStream::prepare_connect(physical);
    if (prepared.is_err()) return Err(lifetchto::Error::Connect(prepared.unwrap_err()));
    {
        auto f = stats->lock().unwrap();
        ++f->prepared;
        f->destination = Some(destination);
    }
    return Ok(Arc<rstd::dyn<lifetchto::http::TcpDial>>::make(
        Dial { Arc<rstd::sync::Mutex<Option<rstd::net::TcpConnect>>>::make(
                   Some(rstd::move(prepared).unwrap())),
               stats.clone(),
               delay,
               cleanup,
               fail_finish,
               fail_take,
               fail_close }));
}
inline auto
route(const lihttpto::Url& url, SocketAddr destination, Connector connector, u64 generation = {})
    -> Arc<lifetchto::http::Route> {
    Vec<SocketAddr> addresses;
    addresses.push(rstd::move(destination));
    auto provider = Arc<rstd::dyn<lifetchto::http::TcpConnector>>::make(rstd::move(connector));
    return Arc<lifetchto::http::Route>::make(
        lifetchto::http::Route::tcp(
            url, rstd::move(addresses), generation, Some(rstd::move(provider)))
            .unwrap());
}
} // namespace test_connector
