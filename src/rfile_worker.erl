% @hidden
-module(rfile_worker).
-behaviour(gen_server).

%% API
-export([start_link/2]).

%% gen_server callbacks
-export([init/1
         , handle_call/3
         , handle_cast/2
         , handle_info/2
         , terminate/2
         , code_change/3]).

-define(SERVER, ?MODULE).

start_link(Args, Options) ->
  gen_server:start_link(?MODULE, {Args, Options}, []).

% @hidden
init({Args, Options}) ->
  {ok, #{
     args => Args,
     options => Options
    }}.

% @hidden
handle_call(_Request, _From, State) ->
  Reply = ok,
  {reply, Reply, State}.

% @hidden
handle_cast(ls, #{args := #{provider := Provider, source := File}, options := #{from := From} = Options} = State) ->
  gen_server:reply(From, {ok, self()}),
  terminate(Provider:ls(File, Options)),
  {noreply, State};
handle_cast(rm, #{args := #{provider := Provider, source := File}, options := #{from := From} = Options} = State) ->
  gen_server:reply(From, {ok, self()}),
  terminate(Provider:rm(File, Options)),
  {noreply, State};
handle_cast(copy, #{args := #{provider := Provider, source := Source, destination := Destination}, options := #{from := From} = Options} = State) ->
  gen_server:reply(From, {ok, self()}),
  terminate(Provider:copy(Source, Destination, Options)),
  {noreply, State}.

% @hidden
handle_info(_Info, State) ->
  {noreply, State}.

% @hidden
terminate(_Reason, _State) ->
  ok.

% @hidden
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

terminate(Result) ->
  gen_server:cast(rfile_workers_manager, {terminate_worker, self(), Result}).
