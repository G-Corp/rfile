% @hidden
-module(rfile_workers_manager).
-compile([{parse_transform, lager_transform}]).
-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1
         , handle_call/3
         , handle_cast/2
         , handle_info/2
         , terminate/2
         , code_change/3]).

-define(SERVER, ?MODULE).

start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

% @hidden
init(_Args) ->
  {ok, #{}}.

% @hidden
handle_call({Job, {Action, Args, Options}}, From, State) ->
  case rfile_workers_sup:start_child(Args, (rfile_utils:options_to_map(Options))#{from => From}) of
    {ok, Child} ->
      Ref = erlang:monitor(process, Child),
      ok = gen_server:cast(Child, Action),
      {noreply, State#{Child => #{ref => Ref,
                                  job => Job,
                                  pid => Child,
                                  from => From,
                                  action => Action,
                                  args => Args,
                                  options => rfile_utils:options_to_map(Options)}}};
    _Other ->
      {reply, {error, internal_error}, State}
  end;
handle_call(_Request, _From, State) ->
  Reply = ok,
  {reply, Reply, State}.

% @hidden
handle_cast({terminate_worker, Pid, Response}, State) ->
  case maps:get(Pid, State, undefined) of
    undefined ->
      {noreply, State};
    #{} = WorkerInfos ->
      rfile_workers_sup:stop_child(Pid),
      {noreply, State#{Pid => WorkerInfos#{response => Response}}}
  end;
handle_cast(_Msg, State) ->
  {noreply, State}.

% @hidden
handle_info({'DOWN', MonitorRef, _Type, Pid, Info}, State) ->
  lager:debug("Worker (PID ~p) terminate with reason ~p", [Pid, Info]),
  case maps:get(Pid, State, undefined) of
    undefined ->
      {noreply, State};
    #{ref := MonitorRef, job := Job} = WorkerInfos ->
      erlang:demonitor(MonitorRef),
      case Info of
        shutdown ->
          rfile_utils:apply_callback(WorkerInfos),
          rfile_workers_sup:stop_child(Pid),
          gen_server:cast(rfile_worker_queue, {delete_job, Job});
        _Other ->
          rfile_utils:apply_callback(WorkerInfos#{response => {error, {worker_down, Info}}})
      end,
      {noreply, maps:remove(Pid, State)}
  end;
handle_info(_Info, State) ->
  {noreply, State}.

% @hidden
terminate(_Reason, _State) ->
  ok.

% @hidden
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.
