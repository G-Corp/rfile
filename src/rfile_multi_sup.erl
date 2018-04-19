% @hidden
-module(rfile_multi_sup).
-behaviour(supervisor).

-export([
         start_link/0
         , stop_child/1
         , start_child/4
        ]).
-export([init/1]).

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

stop_child(Pid) when is_pid(Pid) ->
  supervisor:terminate_child(?MODULE, Pid).

start_child(Action, JobData, Options, Ref) ->
  case supervisor:start_child(?MODULE, [Action, JobData, Options, Ref]) of
    {ok, Child, _} -> {ok, Child};
    Other -> Other
  end.

init([]) ->
  {ok, {
     #{strategy => simple_one_for_one,
       intensity => 0,
       period => 1},
     [
      #{id => rfile_multi,
        start => {rfile_multi, start_link, []},
        type => worker,
        shutdown => 5000}
     ]
    }}.
