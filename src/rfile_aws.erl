% @hidden
-module(rfile_aws).
-behaviour(rfile_storage).
-compile([{parse_transform, lager_transform}]).
-include_lib("erlcloud/include/erlcloud_aws.hrl").
-export([
         ls/2
         , cp/3
         , rm/2
        ]).

-export([
         copy_file_s3_to_s3/6
         , copy_directory_s3_to_s3/6
        ]).

ls(File, Options) ->
  AwsConfig = get_aws_config(Options, source),
  AwsPrefix = get_prefix(File),
  try
    AwsResponse = erlcloud_s3:list_objects(
                    get_bucket(File),
                    [{delimiter, "/"}, {prefix, AwsPrefix}],
                    AwsConfig),
    {ok,
     #{directories => get_response_elements(AwsResponse, common_prefixes, prefix, AwsPrefix),
       files => get_response_elements(AwsResponse, contents, key, AwsPrefix)}}
  catch
    _:{aws_error, {http_error, 404 , _, _}} ->
      {error, bucket_not_found};
    Error:Reason ->
      lager:error("S3 list bucket error: ~p:~p", [Error, Reason]),
      {error, Reason}
  end.

rm(File, Options) ->
  AwsConfig = get_aws_config(Options, source),
  Bucket = get_bucket(File),
  case get_prefix(File) of
    Key when length(Key) > 0 ->
      try
        case rfile_utils:get_path_format(File) of
          {directory, Bucket, Key} ->
            delete_s3_directory(
              Bucket,
              Key,
              Options,
              AwsConfig);
          {file, Bucket, Key} when length(Key) > 0 ->
            delete_s3_file(
              Bucket,
              Key,
              AwsConfig);
          _Other ->
            {error, invalid_source}
        end
      catch
        _:{aws_error, {http_error, 404 , _, _}} ->
          {error, bucket_not_found};
        Error:Reason ->
          lager:error("S3 list bucket error: ~p:~p", [Error, Reason]),
          {error, Reason}
      end;
    _Key ->
      {error, cant_delete_bucket}
  end.

cp(#{type := aws} = Source, #{type := aws} = Destination, Options) ->
  case {get_aws_config(Options, source), get_aws_config(Options, destination)} of
    {AwsConfig, AwsConfig} ->
      case {rfile_utils:get_path_format(Source), rfile_utils:get_path_format(Destination)} of
        {{directory, SourceBucket, SourceKey}, {directory, DestinationBucket, DestinationKey}} ->
          copy_directory_s3_to_s3(SourceBucket, SourceKey, DestinationBucket, DestinationKey, Options, AwsConfig);
        {{directory, _SourceBucket, _SourceKey}, {file, _DestinationBucket, _DestinationKey}} ->
          {error, invalid_destination};
        {{file, SourceBucket, SourceKey}, {directory, DestinationBucket, DestinationKey}} ->
          copy_file_s3_to_s3(SourceBucket, SourceKey, DestinationBucket, add_file(DestinationKey, SourceKey), Options, AwsConfig);
        {{file, SourceBucket, SourceKey}, {file, DestinationBucket, DestinationKey}} ->
          copy_file_s3_to_s3(SourceBucket, SourceKey, DestinationBucket, DestinationKey, Options, AwsConfig)
      end;
    {_, _} ->
      {error, not_supported} % TODO copy from one AWS to an other
  end;
cp(#{type := aws} = Source, #{type := fs} = Destination, Options) ->
  AwsConfig = get_aws_config(Options, source),
  case rfile_utils:get_path_format(Source) of
    {directory, SourceBucket, SourceKey} ->
      copy_directory_s3_to_fs(SourceBucket, SourceKey, rfile_utils:get_filepath(Destination), Options, AwsConfig);
    {file, SourceBucket, SourceKey} ->
      copy_file_s3_to_fs(SourceBucket, SourceKey, rfile_utils:get_filepath(Destination), Options, AwsConfig)
  end;
cp(#{type := fs} = Source, #{type := aws} = Destination, Options) ->
  AwsConfig = get_aws_config(Options, destination),
  case rfile_utils:get_path_format(Destination) of
    {directory, DestinationBucket, DestinationKey} ->
      copy_directory_fs_to_s3(rfile_utils:get_filepath(Source), DestinationBucket, DestinationKey, Options, AwsConfig);
    {file, DestinationBucket, DestinationKey} ->
      copy_file_fs_to_s3(rfile_utils:get_filepath(Source), DestinationBucket, DestinationKey, Options, AwsConfig)
  end;
cp(_Source, _Destination, _Options) ->
  {error, not_supported}.

% ---------------------------------------------------------------------------------------------------------------------

copy_directory_s3_to_fs(SourceBucket, SourceKey, Destination, Options, AwsConfig) ->
  AwsResponse = erlcloud_s3:list_objects(SourceBucket, [{prefix, SourceKey}], AwsConfig),
  case copy_directory_s3_to_fs(
         get_response_elements(AwsResponse, contents, key, SourceKey),
         SourceBucket,
         SourceKey,
         Destination,
         Options,
         AwsConfig) of
    ok ->
      {ok, Destination};
    Error ->
      Error
  end.

copy_directory_s3_to_fs([], _SourceBucket, _SourceKey, _Destination, _Options, _AwsConfig) ->
  ok;
copy_directory_s3_to_fs([File|Files], SourceBucket, SourceKey, Destination, Options, AwsConfig) ->
  case copy_file_s3_to_fs(
         SourceBucket,
         bucuri:join([SourceKey, File]),
         filename:join([Destination, File]),
         Options,
         AwsConfig) of
    {ok, _} ->
      copy_directory_s3_to_fs(Files, SourceBucket, SourceKey, Destination, Options, AwsConfig);
    Error ->
      Error
  end.

copy_directory_fs_to_s3(Source, DestinationBucket, DestinationKey, #{recursive := true} = Options, AwsConfig) ->
  case copy_directory_fs_to_s3(
         filelib:wildcard(bucs:to_string(filename:join([Source, "**", "*"]))),
         Source,
         DestinationBucket,
         DestinationKey,
         Options,
         AwsConfig) of
    ok ->
      {ok, "s3://" ++ DestinationBucket ++ "/" ++ DestinationKey};
    Error ->
      Error
  end;
copy_directory_fs_to_s3(_Source, DestinationBucket, DestinationKey, _Options, _AwsConfig) ->
  {ok, "s3://" ++ DestinationBucket ++ "/" ++ DestinationKey}.

copy_directory_fs_to_s3([], _Source, _DestinationBucket, _DestinationKey, _Options, _AwsConfig) ->
  ok;
copy_directory_fs_to_s3([File|Files], Source, DestinationBucket, DestinationKey, Options, AwsConfig) ->
  case filelib:is_dir(File) of
    true ->
      copy_directory_fs_to_s3(Files, Source, DestinationBucket, DestinationKey, Options, AwsConfig);
    false ->
      case copy_file_fs_to_s3(File, DestinationBucket, s3_file(File, Source, DestinationKey), Options, AwsConfig) of
        {ok, _} ->
          copy_directory_fs_to_s3(Files, Source, DestinationBucket, DestinationKey, Options, AwsConfig);
        Error ->
          Error
      end
  end.

s3_file(File, Source, DestinationKey) ->
  bucs:to_string(bucuri:join(DestinationKey, bucfile:relative_from(File, Source))).

copy_file_s3_to_fs(SourceBucket, SourceKey, Destination, _Options, AwsConfig) ->
  lager:info("COPY s3://~ts/~ts TO ~ts", [SourceBucket, SourceKey, Destination]),
  try
    Data = erlcloud_s3:get_object(SourceBucket, SourceKey, AwsConfig),
    case bucfile:make_dir(filename:dirname(Destination)) of
      ok ->
        case file:write_file(Destination, proplists:get_value(content, Data)) of
          ok ->
            {ok, Destination};
          FSError ->
            FSError
        end;
      FSError ->
        FSError
    end
  catch
    _:{aws_error, {http_error, 404 , _, _}} ->
      {error, invalide_file};
    Error:Reason ->
      lager:error("S3 list bucket error: ~p:~p", [Error, Reason]),
      {error, Reason}
  end.

copy_file_fs_to_s3(Source, DestinationBucket, DestinationKey, Options, AwsConfig) ->
  lager:debug("COPY ~ts TO s3://~ts/~ts", [Source, DestinationBucket, DestinationKey]),
  AwsOptions = aws_options(Options, [acl]),
  case file:read_file(Source) of
    {ok, Data} ->
      try
        erlcloud_s3:put_object(DestinationBucket, DestinationKey, Data, AwsOptions, AwsConfig),
        {ok, "s3://" ++ DestinationBucket ++ "/" ++ DestinationKey}
      catch
        _:_->
          {error, destination_error}
      end;
    FSError ->
      FSError
  end.

copy_file_s3_to_s3(SourceBucket, SourceKey, DestinationBucket, DestinationKey, Options, AwsConfig) ->
  lager:debug("COPY s3://~ts/~ts TO s3://~ts/~ts", [SourceBucket, SourceKey, DestinationBucket, DestinationKey]),
  AwsOptions = aws_options(Options, [acl]),
  try
    erlcloud_s3:copy_object(
      DestinationBucket, DestinationKey,
      SourceBucket, SourceKey,
      AwsOptions,
      AwsConfig),
    {ok, "s3://" ++ DestinationBucket ++ "/" ++ DestinationKey}
  catch
    _:{aws_error, {http_error, 404 , _, _}} ->
      {error, invalide_file};
    Error:Reason ->
      lager:error("S3 list bucket error: ~p:~p", [Error, Reason]),
      {error, Reason}
  end.

copy_directory_s3_to_s3(SourceBucket, SourceKey, DestinationBucket, DestinationKey, Options, AwsConfig) ->
  lager:debug("Copy directory => ~p / ~p to ~p / ~p", [SourceBucket, SourceKey, DestinationBucket, DestinationKey]),
  AwsOptions = aws_options(Options, [acl]),
  try
    case maps:get(copy_files_only, Options, true) of
      false ->
        erlcloud_s3:copy_object(
          DestinationBucket, DestinationKey,
          SourceBucket, SourceKey,
          AwsOptions,
          AwsConfig);
      true ->
        ok
    end,
    case maps:get(recursive, Options, false) of
      true ->
        case copy_recursive_s3_to_s3(SourceBucket, SourceKey, DestinationBucket, DestinationKey, Options, AwsConfig) of
          ok ->
            {ok, "s3://" ++ DestinationBucket ++ "/" ++ DestinationKey};
          Other ->
            Other
        end;
      false ->
        {ok, "s3://" ++ DestinationBucket ++ "/" ++ DestinationKey}
    end
  catch
    _:{aws_error, {http_error, 404 , _, _}} ->
      {error, invalide_file};
    Error:Reason ->
      lager:error("S3 list bucket error: ~p:~p", [Error, Reason]),
      {error, Reason}
  end.

copy_recursive_s3_to_s3(SourceBucket, SourceKey, DestinationBucket, DestinationKey, Options, AwsConfig) ->
  AwsResponse = erlcloud_s3:list_objects(SourceBucket, [{delimiter, "/"}, {prefix, SourceKey}], AwsConfig),
  case copy_objects_s3_to_s3(
         get_response_elements(AwsResponse, contents, key, SourceKey),
         SourceBucket, SourceKey, DestinationBucket, DestinationKey, Options, AwsConfig, copy_file_s3_to_s3) of
    ok ->
      case copy_objects_s3_to_s3(
             get_response_elements(AwsResponse, common_prefixes, prefix, SourceKey),
             SourceBucket, SourceKey, DestinationBucket, DestinationKey, Options, AwsConfig, copy_directory_s3_to_s3) of
        ok -> ok;
        Error -> Error
      end;
    Error -> Error
  end.

copy_objects_s3_to_s3([], _SourceBucket, _SourceKey, _DestinationBucket, _DestinationKey, _Options, _AwsConfig, _Fun) ->
  ok;
copy_objects_s3_to_s3([File|Rest], SourceBucket, SourceKey, DestinationBucket, DestinationKey, Options, AwsConfig, Fun) ->
  lager:debug("~p ~p / ~p~p to ~p / ~p~p", [Fun, SourceBucket, SourceKey, File, DestinationBucket, DestinationKey, File]),
  case erlang:apply(?MODULE, Fun, [SourceBucket, SourceKey ++ File, DestinationBucket, DestinationKey ++ File, Options, AwsConfig]) of
    {ok, _} ->
      copy_objects_s3_to_s3(Rest, SourceBucket, SourceKey, DestinationBucket, DestinationKey, Options, AwsConfig, Fun);
    Error ->
      Error
  end.

aws_options(Options, List) ->
  lists:foldl(fun(Option, Acc) ->
                  case maps:get(Option, Options, undefined) of
                    undefined -> Acc;
                    Value -> [{Option, Value}|Acc]
                  end
              end, [], List).

add_file(DestinationKey, SourceKey) ->
  DestinationKey ++ filename:basename(SourceKey).

get_bucket(#{host := [], path := Path}) ->
  Path;
get_bucket(#{host := Host}) ->
  Host.

get_prefix(#{host := []}) ->
  "";
get_prefix(#{path := Path}) ->
  Path.

get_aws_config(Options, Who) ->
  (case get_aws_credentials(Options, Who) of
     #{access_key_id := AccessKeyID, secret_access_key := SecretAccessKey} ->
       erlcloud_s3:new(AccessKeyID, SecretAccessKey);
     _ ->
       #aws_config{}
   end)#aws_config{s3_follow_redirect = true}.

get_aws_credentials(#{aws := Aws} = Options, Who) ->
  case maps:get(Who, Options, #{}) of
    #{aws := SourceAws} ->
      maps:merge(Aws, SourceAws);
    _Other ->
      Aws
  end;
get_aws_credentials(Options, Who) ->
  case maps:get(Who, Options, #{}) of
    #{aws := SourceAws} ->
      SourceAws;
    _Other ->
      undefined
  end.

get_response_elements(AwsResponse, Global, Local, AwsPrefix) ->
  case lists:keyfind(Global, 1, AwsResponse) of
    false ->
      [];
    {Global, Prefixes} ->
      lists:foldr(fun(Prefix, Acc) ->
                      case lists:keyfind(Local, 1, Prefix) of
                        {Local, Data} ->
                          case re:replace(Data, AwsPrefix, "", [{return, list}]) of
                            "" -> Acc;
                            Elem -> [Elem|Acc]
                          end;
                        false ->
                          Acc
                      end
                  end, [], Prefixes)
  end.

delete_s3_file(Bucket, Key, AwsConfig) ->
  lager:debug("DELETE s3://~ts/~ts", [Bucket, Key]),
  erlcloud_s3:delete_object(Bucket, Key, AwsConfig),
  {ok, "s3://" ++ Bucket ++ "/" ++ Key}.

delete_s3_directory(Bucket, Key, Options, AwsConfig) ->
  Recursive = maps:get(recursive, Options, false),
  AwsResponse = erlcloud_s3:list_objects(Bucket, [{delimiter, "/"}, {prefix, Key}], AwsConfig),
  Keys = get_response_elements(AwsResponse, contents, key, Key),
  Prefixes = get_response_elements(AwsResponse, common_prefixes, prefix, Key),
  case (((length(Keys) == 0) and (length(Prefixes) == 0)) or Recursive) of
    true ->
      lists:foreach(fun(File) ->
                        delete_s3_file(Bucket, Key ++ File, AwsConfig)
                    end, Keys),
      lists:foreach(fun(Dir) ->
                        delete_s3_directory(Bucket, Key ++ Dir, Options, AwsConfig)
                    end, Prefixes),
      lager:debug("DELETE s3://~ts/~ts", [Bucket, Key]),
      erlcloud_s3:delete_object(Bucket, Key, AwsConfig),
      {ok, "s3://" ++ Bucket ++ "/" ++ Key};
    false ->
      {error, not_empty}
  end.
