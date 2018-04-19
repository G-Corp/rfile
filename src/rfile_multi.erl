% @hidden
-module(rfile_multi).
-compile([{parse_transform, lager_transform}]).
-behaviour(gen_server).

%% API
-export([start_link/4]).

%% gen_server callbacks
-export([init/1
         , handle_call/3
         , handle_cast/2
         , handle_info/2
         , terminate/2
         , code_change/3]).

start_link(Action, JobData, Options, Ref) ->
  gen_server:start_link(?MODULE, [Action, JobData, Options, Ref], []).

% @hidden
init([Action, #{destination := Destinations} = JobData, Options, Ref]) ->
  Jobs = [
           begin
             JRef = erlang:make_ref(),
             gen_server:cast(
               rfile_workers_queue,
               {{
                 Action,
                 JobData#{destination => Destination},
                 Options#{callback => self()}
                }, Ref
               }
              ),
             JRef
           end
           || Destination <- Destinations
          ],
  gen_server:cast(rfile_workers_queue, {register_multi, Ref, Jobs}),
  {ok, #{
     action => Action,
     job_data => JobData,
     todo => length(Jobs),
     jobs => Jobs,
     options => Options,
     ok => [],
     errors => [],
     ref => Ref
    }}.

% @hidden
handle_call(_Request, _From, State) ->
  Reply = ok,
  {reply, Reply, State}.

% @hidden
handle_cast(_Msg, State) ->
  {noreply, State}.

% @hidden
handle_info({response, Action, Files, {error, Error}, _Metadata},
            #{action := Action, todo := Todo, errors := Errors} = State) ->

  {
   noreply,
   check_terminate(
     State#{
       errors => [{lists:last(Files), Error}|Errors],
       todo => Todo - 1
      }
    )
  };
handle_info({response, Action, _Files, {ok, File}, _Metadata},
            #{action := Action, todo := Todo, ok := OK} = State) ->
  {
   noreply,
   check_terminate(
     State#{
       ok => [File|OK],
       todo => Todo - 1
      }
    )
  }.

check_terminate(#{todo := 0,
                  options := Options,
                  ok := OK,
                  errors := Errors,
                  action := Action,
                  job_data := Args,
                  ref := Ref} = State) ->
  rfile_utils:apply_callback(
    #{options => Options,
      response => case Errors of
                    [] -> {ok, OK};
                    _ -> {error, {OK, Errors}}
                  end,
      action => Action,
      args => Args}),
  gen_server:cast(rfile_workers_queue, {terminate_multi, self(), Ref}),
  State;
check_terminate(State) ->
  State.

% @hidden
terminate(_Reason, _State) ->
  ok.

% @hidden
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.
