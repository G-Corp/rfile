% @hidden
-module(rfile_workers_sup).
-behaviour(supervisor).

-export([
         start_link/0
         , stop_child/1
         , start_child/2
        ]).
-export([init/1]).

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

stop_child(Pid) when is_pid(Pid) ->
  supervisor:terminate_child(?MODULE, Pid).

start_child(Args, Options) ->
  case supervisor:start_child(?MODULE, [Args, Options]) of
    {ok, Child, _} -> {ok, Child};
    Other -> Other
  end.

init([]) ->
  {ok, {
     #{strategy => simple_one_for_one,
       intensity => 0,
       period => 1},
     [
      #{id => rfile_worker,
         start => {rfile_worker, start_link, []},
         type => worker,
         shutdown => 5000}
     ]
    }}.
