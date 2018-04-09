% @hidden
-module(rfile_utils).

-export([
         get_filepath/1
         , get_path_format/1
        ]).

get_filepath(#{host := [], path := Path}) ->
  Path;
get_filepath(#{host := Root, path := Path}) ->
  filename:join(Root, Path).

get_path_format(#{host := [], path := Path}) ->
  {directory, Path, ""};
get_path_format(#{host := Host, path := Path}) ->
  case lists:reverse(Path) of
    [$/|_] ->
      {directory, Host, Path};
    _ ->
      {file, Host, Path}
  end.
