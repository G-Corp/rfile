% @hidden
-module(rfile_worker_queue).
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
     jobs => #{},
     timer => erlang:send_after(?INITIAL_START_INTERVAL, self(), start_job),
     queue => queue:new(),
     active_jobs => #{},
     max_jobs => ?DEFAULT_MAX_JOBS
    }}.

% @hidden
handle_call({_, _, _} = Job, _From, #{queue := Queue, jobs := Jobs} = State) ->
  Ref = erlang:make_ref(),
  {reply, {ok, Ref}, State#{queue => queue:in(Ref, Queue), jobs => maps:put(Ref, Job, Jobs)}};
handle_call(jobs, _From, #{queue := Queue} = State) ->
  {reply, queue:len(Queue), State};
handle_call({job, Job}, _From, State) ->
  {reply, get_job_status(Job, State), State};
handle_call(_Request, _From, State) ->
  Reply = ok,
  {reply, Reply, State}.

% @hidden
handle_cast({max_jobs, MaxJobs}, State) ->
  {noreply, start_job(State#{max_jobs => MaxJobs})};
handle_cast({delete_job, Job}, #{jobs := Jobs, active_jobs := ActiveJobs} = State) ->
  case maps:is_key(Job, ActiveJobs) of
    true ->
      {noreply, start_job(State#{jobs => maps:remove(Job, Jobs), active_jobs => maps:remove(Job, ActiveJobs)})};
    false ->
      {noreply, State}
  end;
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

get_job_status(#{queue := Queue, active_jobs := ActiveJobs}, Job) ->
  case maps:is_key(Job, ActiveJobs) of
    true ->
      started;
    false ->
      case queue:member(Job, Queue) of
        true ->
          queued;
        false ->
          terminated
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
              case gen_server:call(rfile_workers_manager, {JobRef, Job}) of
                {ok, WorkerPid} ->
                  State#{queue => NewQueue, active_jobs := Jobs#{JobRef => WorkerPid}};
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
