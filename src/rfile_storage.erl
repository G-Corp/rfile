-module(rfile_storage).

-callback ls(File :: map(), Options :: rfile:options()) ->
  {ok, #{files => [string()], directories => [string()]}} | {error, term()}.

-callback copy(Spurce :: map(), Destination :: map(), Options :: rfile:options()) ->
  {ok, string()} | {error, term()}.
