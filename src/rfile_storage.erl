-module(rfile_storage).

-callback ls(File :: map(), Options :: rfile:options()) ->
  {ok, #{files => [string()], directories => [string()]}} | {error, term()}.

-callback rm(File :: map(), Options :: rfile:options()) ->
  {ok, string()} | {error, term()}.

-callback cp(Source :: map(), Destination :: map(), Options :: rfile:options()) ->
  {ok, string()} | {error, term()}.

-callback diff(Source :: map(), Destination :: map(), Options :: rfile:options()) ->
  {ok, #{miss => #{source => [string()],
                   destination => [string()]},
         differ => [string()]}} | {error, term()}.
