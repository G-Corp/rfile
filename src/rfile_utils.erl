% @hidden
-module(rfile_utils).

-export([
         get_filepath/1
         , get_path_format/1
         , options_to_map/1
         , apply_callback/1
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

options_to_map(Options) when is_map(Options) ->
  Options;
options_to_map(Options) when is_list(Options) ->
  bucmaps:from_list(Options).

apply_callback(#{options := #{callback := Callback} = Options,
                 response := Response,
                 action := Action,
                 args := #{source := #{file := SrcFile}} = Args}) ->
  erlang:apply(
    Callback,
    [Action,
     case maps:get(destination, Args, undefined) of
       #{file := DestFile} ->
         [SrcFile, DestFile];
       _ ->
         [SrcFile]
     end,
     Response,
     maps:get(metadata, Options, undefined)]);
apply_callback(_WorkerInfos) ->
  ok.
