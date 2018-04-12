-module(rfile_storage).

-callback ls(File :: map(), Options :: rfile:options()) ->
  {ok, #{files => [string()], directories => [string()]}} | {error, term()}.

-callback rm(File :: map(), Options :: rfile:options()) ->
  {ok, string()} | {error, term()}.

-callback cp(Spurce :: map(), Destination :: map(), Options :: rfile:options()) ->
  {ok, string()} | {error, term()}.
