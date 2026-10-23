#pragma once
import lifetchto;
using namespace rstd::prelude;
using namespace rstd::literals;
namespace test_http
{
using lifetchto::Arc;
using lifetchto::Bytes;
using lifetchto::coro;
using lifetchto::Duration;
using lifetchto::Instant;
using lifetchto::http::Route;
using lifetchto::http::Resolver;
using lifetchto::http::TlsProfile;
struct ExchangeOptions {Option<Instant> deadline;Option<Arc<Route>> route;};
struct TestClientOptions:lifetchto::http::TransportOptions{Option<Duration> total_timeout{Some(Duration::from_secs(u64(30)))};};
struct TestUploadBody{
    lifetchto::UploadPlan content;
    Option<lihttpto::Headers> trailers;
    static auto bytes(Bytes bytes)->TestUploadBody{return {lifetchto::UploadPlan::bytes(rstd::move(bytes)),{}};}
    template<class S>static auto from_source(S source,Option<u64> length={})->TestUploadBody{return {lifetchto::UploadPlan::once(lifetchto::UploadBody::from_source(rstd::move(source),length)),{}};}
    void set_trailers(lihttpto::Headers value){trailers=Some(rstd::move(value));}
};
class TestRequest{
    lihttpto::RequestParts parts_;
    TestUploadBody body_;
public:
    explicit TestRequest(lihttpto::RequestParts parts):parts_(rstd::move(parts)){}
    static auto make(ref<str> url,ref<str> method="GET"_str)->lifetchto::Result<TestRequest>{
        auto parsed=lihttpto::Url::parse_http(url);auto verb=lihttpto::Method::parse(method);
        if(parsed.is_err()||verb.is_err())return Err(lifetchto::Error::InvalidOptions(String::make("invalid test request"_str)));
        return Ok(TestRequest({rstd::move(parsed).unwrap(),rstd::move(verb).unwrap(),{}}));
    }
    auto parts()->lihttpto::RequestParts&{return parts_;}
    auto body()->TestUploadBody&{return body_;}
    auto into_message()&&->lifetchto::http::ExecutionRequest{return {rstd::move(parts_),lifetchto::http::UploadPlan(rstd::move(body_.content),body_.trailers.take())};}
};
struct TestBodyState{
    lifetchto::Transfer<lifetchto::http::ResponseMessage> transfer;
    lifetchto::http::ResponseBody body;
    Arc<lifetchto::Engine> engine;
    TestBodyState(lifetchto::Transfer<lifetchto::http::ResponseMessage> transfer,lifetchto::http::ResponseBody body,Arc<lifetchto::Engine> engine):transfer(rstd::move(transfer)),body(rstd::move(body)),engine(rstd::move(engine)){}
};
inline auto test_next(Arc<TestBodyState> state)->coro<lifetchto::Result<Option<Bytes>>>{
    auto result=co_await state->body.next();if(result.is_ok()&&result->is_none())(void)co_await state->transfer.control().wait();co_return rstd::move(result);
}
struct TestCollector{using Error=lifetchto::Error;Arc<TestBodyState> state;auto next(){return test_next(state.clone());}};
inline auto test_collect(Arc<TestBodyState> state,usize limit)->coro<lifetchto::Result<Bytes>>{
    auto result=co_await lihttpto::collect_body(TestCollector{state.clone()},limit);
    if(result.is_err()){state->body.cancel();auto e=rstd::move(result).unwrap_err();if(e.is_Source())co_return Err(rstd::move(e.as_Source().error));co_return Err(lifetchto::Error::TooLarge());}
    co_return Ok(rstd::move(result).unwrap());
}
inline auto test_text(Arc<TestBodyState> state,usize limit)->coro<lifetchto::Result<String>>{
    auto result=co_await test_collect(rstd::move(state),limit);if(result.is_err())co_return Err(rstd::move(result).unwrap_err());auto text=rstd::str_::from_utf8(result->as_slice());if(text.is_err())co_return Err(lifetchto::Error::BodySource(String::make("invalid UTF-8"_str)));co_return Ok(String::make(*text));
}
class TestBody{
    Arc<TestBodyState> state_;
public:
    explicit TestBody(Arc<TestBodyState> state):state_(rstd::move(state)){}
    TestBody(TestBody&&)noexcept=default;
    TestBody(const TestBody&)=delete;
    auto operator=(TestBody&& other)noexcept->TestBody&{if(this!=&other){cancel();state_=rstd::move(other.state_);}return *this;}
    ~TestBody(){cancel();}
    auto next()->coro<lifetchto::Result<Option<Bytes>>>{return test_next(state_.clone());}
    auto bytes(usize limit)->coro<lifetchto::Result<Bytes>>{return test_collect(state_.clone(),limit);}
    auto text(usize limit)->coro<lifetchto::Result<String>>{return test_text(state_.clone(),limit);}
    auto trailers()const->lifetchto::Result<Option<lihttpto::Headers>>{return state_->body.trailers();}
    void cancel(){if(state_)state_->body.cancel();}
};
class TestResponse{
    lihttpto::ResponseHead head_;
    Option<TestBody> body_;
    Option<Instant> deadline_;
public:
    TestResponse(lihttpto::ResponseHead head,TestBody body,Option<Instant> deadline):head_(rstd::move(head)),body_(Some(rstd::move(body))),deadline_(deadline){}
    auto head()const->const lihttpto::ResponseHead&{return head_;}
    auto take_body()->lifetchto::Result<TestBody>{if(body_.is_none())return Err(lifetchto::Error::Busy());return Ok(body_.take().unwrap());}
    auto deadline()const->Option<Instant>{return deadline_;}
};
struct TestClientState{
    Arc<lifetchto::Engine> engine;
    lifetchto::http::Client client;
    Option<Duration> timeout;
    TestClientState(Arc<lifetchto::Engine> engine,lifetchto::http::Client client,Option<Duration> timeout):engine(rstd::move(engine)),client(rstd::move(client)),timeout(timeout){}
};
inline auto test_response(Arc<lifetchto::Engine> engine,lifetchto::Transfer<lifetchto::http::ResponseMessage> transfer,Option<Instant> deadline)->coro<lifetchto::Result<TestResponse>>{
    auto result=co_await transfer.output();if(result.is_err())co_return Err(rstd::move(result).unwrap_err());auto message=rstd::move(result).unwrap();auto body=Arc<TestBodyState>::make(rstd::move(transfer),rstd::move(message.body),engine.clone());co_return Ok(TestResponse(rstd::move(message.head),TestBody(rstd::move(body)),deadline));
}
inline auto test_rejected(lifetchto::Rejected<lifetchto::http::ExecutionRequest> rejected)->coro<lifetchto::Result<TestResponse>>{(void)co_await rejected.input.body.close();co_return Err(rstd::move(rejected.error));}
inline auto test_close(Arc<TestClientState> state)->coro<lifetchto::Result<empty>>{
    auto client=co_await state->client.close();auto engine=co_await state->engine->close();if(client.is_err())co_return Err(client.errors[usize()].clone());if(engine.is_err())co_return Err(engine.errors[usize()].clone());co_return Ok(empty{});
}
class TestClient{
    Arc<TestClientState> state_;
    explicit TestClient(Arc<TestClientState> state):state_(rstd::move(state)){}
public:
    static auto make(rstd::async::RuntimeHandle runtime,TestClientOptions options={},lifetchto::http::ClientConfig config={})->lifetchto::Result<TestClient>{
        auto created=lifetchto::Engine::make(rstd::move(runtime));if(created.is_err())return Err(rstd::move(created).unwrap_err());auto engine=Arc<lifetchto::Engine>::make(rstd::move(created).unwrap());auto timeout=options.total_timeout;config.total_timeout=timeout;
        auto backend=lifetchto::http::NativeBackend::make(engine->resources(),rstd::move(static_cast<lifetchto::http::TransportOptions&>(options)));if(backend.is_err())return Err(rstd::move(backend).unwrap_err());
        auto client=lifetchto::http::Client::make(engine->handle(),lifetchto::http::BackendBinding::Owned(backend->backend()),rstd::move(config));if(client.is_err())return Err(rstd::move(client).unwrap_err());
        return Ok(TestClient(Arc<TestClientState>::make(rstd::move(engine),rstd::move(client).unwrap(),timeout)));
    }
    auto clone()const->TestClient{return TestClient(state_.clone());}
    auto send(TestRequest request,ExchangeOptions options={})const->coro<lifetchto::Result<TestResponse>>{
        auto deadline=options.deadline;if(state_->timeout.is_some()){auto own=Instant::now()+*state_->timeout;if(deadline.is_none()||own<*deadline)deadline=Some(own);}
        lifetchto::http::ExecuteOptions execute;execute.transfer.deadline=deadline;execute.routing.route=options.route.clone();
        auto submitted=state_->client.execute(rstd::move(request).into_message(),rstd::move(execute));if(submitted.is_err())return test_rejected(rstd::move(submitted).unwrap_err());return test_response(state_->engine.clone(),rstd::move(submitted).unwrap(),deadline);
    }
    auto exchange(TestRequest request,ExchangeOptions options={})const->coro<lifetchto::Result<TestResponse>>{return send(rstd::move(request),rstd::move(options));}
    void request_close()const{state_->client.request_close();}
    auto close()const->coro<lifetchto::Result<empty>>{request_close();return test_close(state_.clone());}
};
}
