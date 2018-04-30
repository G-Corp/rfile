% @hidden
-module(rfile_utils).
-compile([{parse_transform, lager_transform}]).

-export([
         get_filepath/1
         , get_path_format/1
         , options_to_map/1
         , apply_callback/1
        ]).

-define(DEFAULT_OPTIONS, #{retry_on_error => 2}).

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
  maps:merge(?DEFAULT_OPTIONS, Options);
options_to_map(Options) when is_list(Options) ->
  options_to_map(bucmaps:from_list(Options)).

apply_callback(#{options := #{retry_on_error := N} = Options,
                 response := {error, Error},
                 job := JobRef,
                 action := Action,
                 args := Args}) when N > 0 ->
  lager:error("~ts error : ~p -> retry ~p", [format_action_error(Action, Args), Error, N]),
  gen_server:cast(rfile_workers_queue, {{Action, Args, Options#{retry_on_error => N - 1}}, JobRef});
apply_callback(#{options := #{callback := Callback} = Options,
                 response := Response,
                 action := Action,
                 args := #{source := #{file := SrcFile}} = Args}) when is_function(Callback, 4) ->
  erlang:apply(
    Callback,
    [Action,
     case maps:get(destination, Args, undefined) of
       Destinations when is_list(Destinations) ->
         [SrcFile, [File || #{file := File} <- Destinations]];
       #{file := DestFile} ->
         [SrcFile, DestFile];
       _ ->
         [SrcFile]
     end,
     Response,
     maps:get(metadata, Options, undefined)]);
apply_callback(#{options := #{callback := Callback} = Options,
                 response := Response,
                 action := Action,
                 args := #{source := #{file := SrcFile}} = Args}) when is_pid(Callback) ->
  Callback ! {
    response,
    Action,
    case maps:get(destination, Args, undefined) of
      #{file := DestFile} ->
        [SrcFile, DestFile];
      _ ->
        [SrcFile]
    end,
    Response,
    maps:get(metadata, Options, undefined)
   };
apply_callback(_WorkerInfos) ->
  ok.

format_action_error(Action, #{source := #{file := Src}, destination := #{file := Dest}}) ->
  lists:flatten(io_lib:format("~ts (~ts -> ~ts)", [Action, Src, Dest]));
format_action_error(Action, #{source := #{file := Src}}) ->
  lists:flatten(io_lib:format("~ts (~ts)", [Action, Src]));
format_action_error(Action, _Args) ->
  Action.
