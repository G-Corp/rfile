% @hidden
-module(rfile_workers_queue).
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
-define(DEFAULT_MAX_JOBS, 5).
-define(INITIAL_START_INTERVAL, 500).
-define(START_INTERVAL, 100).

start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

% @hidden
init(_Args) ->
  {ok, #{
     multi => #{},
     jobs => #{},
     timer => erlang:send_after(?INITIAL_START_INTERVAL, self(), start_job),
     queue => queue:new(),
     active_jobs => #{},
     max_jobs => ?DEFAULT_MAX_JOBS
    }}.

% @hidden
handle_call(jobs, _From, #{queue := Queue} = State) ->
  {reply, queue:len(Queue), State};
handle_call({status, Job}, _From, State) ->
  {reply, get_job_status(Job, State), State};
handle_call(status, _From, State) ->
  {reply, get_all_jobs_status(State), State};
handle_call(_Request, _From, State) ->
  Reply = ok,
  {reply, Reply, State}.

% @hidden
handle_cast({{_, _, _} = Job, Ref}, #{queue := Queue, jobs := Jobs} = State) ->
  {noreply, start_job(State#{queue => queue:in(Ref, Queue), jobs => maps:put(Ref, Job, Jobs)})};
handle_cast({max_jobs, MaxJobs}, State) ->
  {noreply, start_job(State#{max_jobs => MaxJobs})};
handle_cast({delete_job, Job}, #{jobs := Jobs, active_jobs := ActiveJobs} = State) ->
  lager:debug("DELETE JOB ~p", [Job]),
  case maps:is_key(Job, ActiveJobs) of
    true ->
      {noreply, start_job(State#{jobs => maps:remove(Job, Jobs), active_jobs => maps:remove(Job, ActiveJobs)})};
    false ->
      {noreply, State}
  end;
handle_cast({register_multi, Ref, Jobs}, #{multi := Multi} = State) ->
  {noreply, State#{multi => Multi#{Ref => Jobs}}};
handle_cast({terminate_multi, Pid, Ref}, #{multi := Multi} = State) ->
  rfile_multi_sup:stop_child(Pid),
  {noreply, State#{multi => maps:remove(Ref, Multi)}};
handle_cast(_Msg, State) ->
  {noreply, State}.

% @hidden
handle_info(start_job, State) ->
  {noreply, start_job(State)};
handle_info(_Info, State) ->
  {noreply, State}.

% @hidden
terminate(_Reason, _State) ->
  ok.

% @hidden
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

get_job_status(Job, #{queue := Queue, active_jobs := ActiveJobs, multi := Multi} = State) ->
  case maps:is_key(Job, ActiveJobs) of
    true ->
      started;
    false ->
      case queue:member(Job, Queue) of
        true ->
          queued;
        false ->
          case maps:get(Job, Multi, undefined) of
            undefined ->
              terminated;
            Jobs ->
              get_multi_status([get_job_status(J, State) || J <- Jobs])
          end
      end
  end.

get_all_jobs_status(#{jobs := Jobs, queue := Queue, active_jobs := ActiveJobs, multi := Multi}) ->
  MultiJobsRefs = maps:fold(fun(MRef, JobsRefs, Acc) ->
                                lists:foldl(fun(JobRef, Acc0) ->
                                                Acc0#{JobRef => MRef}
                                            end, Acc, JobsRefs)
                            end, #{}, Multi),
  Started = maps:fold(fun(Ref, _WorkerPid, Acc) ->
                          case maps:get(Ref, Jobs, undefined) of
                            {Action, #{source := #{file := File}}, _Options} ->
                              case maps:get(Ref, MultiJobsRefs, undefined) of
                                undefined ->
                                  [{Ref, {Action, File}}|Acc];
                                MRef ->
                                  [{MRef, Ref, {Action, File}}|Acc]
                              end;
                            undefined ->
                              Acc
                          end
                      end, [], ActiveJobs),
  Queued = lists:foldr(fun(Ref, Acc) ->
                           case maps:get(Ref, Jobs) of
                             {Action, #{source := #{file := File}}, _Options} ->
                               case maps:get(Ref, MultiJobsRefs, undefined) of
                                 undefined ->
                                   [{Ref, {Action, File}}|Acc];
                                 MRef ->
                                   [{MRef, Ref, {Action, File}}|Acc]
                               end;
                             _ ->
                               Acc
                           end
                       end, [], queue:to_list(Queue)),
  [{started, Started}, {queued, Queued}].

get_multi_status(Status) ->
  case lists:all(fun(S) -> S =:= terminated end, Status) of
    true -> terminated;
    false ->
      case lists:any(fun(S) -> S =:= started end, Status) of
        true -> started;
        false -> queued
      end
  end.

start_job(#{queue := Queue, jobs := Jobs, active_jobs := ActiveJobs, max_jobs := MaxJobs, timer := Timer} = State) ->
  erlang:cancel_timer(Timer),
  case maps:size(ActiveJobs) < MaxJobs of
    false ->
      State;
    true ->
      case queue:out(Queue) of
        {{value, JobRef}, NewQueue} ->
          case maps:get(JobRef, Jobs, undefined) of
            undefined ->
              State;
            {Action, Args, Options} = Job ->
              case gen_server:call(rfile_workers_manager, {JobRef, Job}, infinity) of
                {ok, WorkerPid} ->
                  lager:debug("START JOB ~p (~p)", [JobRef, WorkerPid]),
                  start_job(State#{queue => NewQueue, active_jobs := ActiveJobs#{JobRef => WorkerPid}});
                {error, Reason} ->
                  rfile_utils:apply_callback(#{
                    options => rfile_utils:options_to_map(Options),
                    action => Action,
                    args => Args,
                    response => {error, Reason}
                   }),
                  start_job(State#{jobs := maps:remove(JobRef, Jobs)})
              end
          end;
        {empty, Queue} ->
          State#{timer => erlang:send_after(?START_INTERVAL, self(), start_job)}
      end
  end.
