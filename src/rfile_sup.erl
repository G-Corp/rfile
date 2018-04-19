% @hidden
-module(rfile_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
  {ok, {
    #{strategy => one_for_one,
      intensity => 1,
      period => 5},
    [
      #{id => rfile_multi_sup,
        start => {rfile_multi_sup, start_link, []},
        type => supervisor,
        shutdown => 5000},
      #{id => rfile_workers_sup,
        start => {rfile_workers_sup, start_link, []},
        type => supervisor,
        shutdown => 5000},
      #{id => rfile_workers_queue,
        start => {rfile_workers_queue, start_link, []},
        type => worker,
        shutdown => 5000},
      #{id => rfile_workers_manager,
        start => {rfile_workers_manager, start_link, []},
        type => worker,
        shutdown => 5000}
    ]
  }}.
