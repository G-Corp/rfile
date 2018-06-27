% @hidden
-module(rfile_aws).
-behaviour(rfile_storage).
-compile([{parse_transform, lager_transform}]).
-include_lib("erlcloud/include/erlcloud_aws.hrl").
-export([
         ls/2
         , cp/3
         , rm/2
         , diff/3
        ]).

-export([
         copy_file_s3_to_s3/6
         , copy_directory_s3_to_s3/6
        ]).

ls(#{file := SrcFile} = File, Options) ->
  AwsConfig = get_aws_config(Options, source),
  AwsPrefix = get_prefix(File),
  try
    {Directories, Files} = list_objects(
                             get_bucket(File),
                             AwsPrefix,
                             AwsConfig),
    {ok,
     #{directories => Directories,
       files => Files}}
  catch
    _:{aws_error, {http_error, 404 , _, _}} ->
      {error, bucket_not_found};
    Error:Reason ->
      lager:error("ls (~ts) error: ~p:~p~n~p", [SrcFile, Error, Reason, erlang:get_stacktrace()]),
      {error, Reason}
  end.

rm(#{file := SrcFile} = File, Options) ->
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
          lager:error("rm (~ts) error: ~p:~p~n~p", [SrcFile, Error, Reason, erlang:get_stacktrace()]),
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

diff(#{type := aws} = Source, #{type := aws} = Destination, Options) ->
  case {get_diff_data(Source, Options),
        get_diff_data(Destination, Options)} of
    {{ok, {SrcDirectries, SrcFiles}},
     {ok, {DestDirectories, DestFiles}}} ->
      {ok, #{miss => #{source => get_miss([{SrcDirectries, DestDirectories}, {SrcFiles, DestFiles}]),
                       destination => get_miss([{DestDirectories, SrcDirectries}, {DestFiles, SrcFiles}])},
             differ => get_differ(SrcFiles, DestFiles)}};
    {{error, _} = Error, _} ->
      Error;
    {_, {error, _} = Error} ->
      Error
  end;
diff(#{type := aws} = _Source, #{type := fs} = _Destination, _Options) ->
  {error, not_implemented};
diff(#{type := fs} = _Source, #{type := aws} = _Destination, _Options) ->
  {error, not_implemented};
diff(_Source, _Destination, _Options) ->
  {error, not_supported}.

get_differ(Left, Right) ->
  get_differ(Left, Right, []).
get_differ([], _Right, Acc) -> Acc;
get_differ([{File, Size}|Left], Right, Acc) ->
  get_differ(
    Left,
    Right,
    case lists:keyfind(File, 1, Right) of
      {File, Size} -> Acc;
      {File, _OtherSize} -> [File|Acc];
      false -> Acc
    end);
get_differ([_|Left], Right, Acc) ->
  get_differ(Left, Right, Acc).

get_miss(Data) ->
  get_miss(Data, []).
get_miss([], Acc) ->
  Acc;
get_miss([{Left, Right}|Rest], Acc) ->
  get_miss(Rest, get_miss(Left, Right, Acc)).
get_miss(_Left, [], Acc) ->
  Acc;
get_miss(Left, [Current|Right], Acc) ->
  case exist(Current, Left) of
    true ->
      get_miss(Left, Right, Acc);
    {false, File} ->
      get_miss(Left, Right, [File|Acc])
  end.

exist({File, _}, List) ->
  case lists:member(File, List) of
    true ->
      true;
    false ->
      case lists:keyfind(File, 1, List) of
        false -> {false, File};
        _ -> true
      end
  end;
exist(File, List) ->
  case lists:member(File, List) of
    true ->
      true;
    false ->
      case lists:keyfind(File, 1, List) of
        false -> {false, File};
        _ -> true
      end
  end.

get_diff_data(#{file := Src} = File, Options) ->
  try
    AwsConfig = get_aws_config(Options, source),
    AwsPrefix = get_prefix(File),
    {ok,
     list_objects_with_size(
       get_bucket(File),
       AwsPrefix,
       AwsConfig)}
  catch
    _:{aws_error, {http_error, 404 , _, _}} ->
      {error, bucket_not_found};
    Error:Reason ->
      lager:error("diff (~ts) error: ~p:~p~n~p", [Src, Error, Reason, erlang:get_stacktrace()]),
      {error, Reason}
  end.

% ---------------------------------------------------------------------------------------------------------------------

copy_directory_s3_to_fs(SourceBucket, SourceKey, Destination, Options, AwsConfig) ->
  {_Directories, Files} = list_objects(SourceBucket, SourceKey, AwsConfig),
  case copy_directory_s3_to_fs(
         Files,
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
      lager:error("cp (s3://~ts/~ts -> ~ts) error: ~p:~p~n~p", [SourceBucket, SourceKey, Destination, Error, Reason, erlang:get_stacktrace()]),
      {error, Reason}
  end.

copy_file_fs_to_s3(Source, DestinationBucket, DestinationKey, Options, AwsConfig) ->
  lager:info("COPY ~ts TO s3://~ts/~ts", [Source, DestinationBucket, DestinationKey]),
  AwsOptions = aws_options(Options, [acl]),
  MaxFileSize = 5000000000,
  FileSize = filelib:file_size(Source),
  if
    FileSize > MaxFileSize ->
      lager:info("Starting multipart upload"),
      do_copy_file_fs_to_s3_multiparts(Source, DestinationBucket, DestinationKey, AwsConfig, AwsOptions);
    true ->
      lager:info("Starting single upload"),
      do_copy_file_fs_to_s3(Source, DestinationBucket, DestinationKey, AwsConfig, AwsOptions)
  end.

do_copy_file_fs_to_s3_multiparts(Source, DestinationBucket, DestinationKey, AwsConfig, AwsOptions) ->
  case erlcloud_s3:start_multipart(DestinationBucket, DestinationKey, AwsOptions, [], AwsConfig) of
    {ok, PropsList} ->
      UploadId = proplists:get_value(uploadId, PropsList),
      case file:open(Source, [read]) of
        {ok, IoDevice} ->
          lager:info("File is open"),
          upload_part(DestinationBucket, DestinationKey, UploadId, 1, AwsOptions, AwsConfig, IoDevice);
        {error, Reason} ->
          erlcloud_s3:abort_multipart(DestinationBucket, DestinationKey, UploadId, AwsOptions, [], AwsConfig),
          {error, Reason}
      end;
    {error, Reason} ->
      {error, Reason}
  end.

upload_part(DestinationBucket, DestinationKey, UploadId, PartNumber, AwsOptions, AwsConfig, IoDevice) ->
  case file:read(IoDevice, 5000000) of
    {ok, Data} ->
      lager:info("Uploading part ~w", [PartNumber]),
      case erlcloud_s3:upload_part(DestinationBucket, DestinationKey, UploadId, PartNumber, Data, [], AwsConfig) of
        {ok, _PropsList} ->
          lager:info("Uploaded part ~w", [PartNumber]),
          upload_part(DestinationBucket, DestinationKey, UploadId, PartNumber + 1, AwsOptions, AwsConfig, IoDevice);
        {error, Reason} ->
          erlcloud_s3:abort_multipart(DestinationBucket, DestinationKey, UploadId, AwsOptions, [], AwsConfig),
          {error, Reason}
      end;
    eof ->
      case file:close(IoDevice) of
        ok ->
          lager:info("File is closed"),
          erlcloud_s3:complete_multipart(DestinationBucket, DestinationKey, UploadId, [], [], AwsConfig);
        {error, Reason} ->
          lager:info("Failed to close the file"),
          erlcloud_s3:abort_multipart(DestinationBucket, DestinationKey, UploadId, AwsOptions, [], AwsConfig),
          {error, Reason}
      end;
    {error, Reason} ->
      erlcloud_s3:abort_multipart(DestinationBucket, DestinationKey, UploadId, AwsOptions, [], AwsConfig),
      {error, Reason}
  end.

do_copy_file_fs_to_s3(Source, DestinationBucket, DestinationKey, AwsConfig, AwsOptions) ->
  case file:read_file(Source) of
    {ok, Data} ->
      try
        erlcloud_s3:put_object(DestinationBucket, DestinationKey, Data, AwsOptions, AwsConfig),
        {ok, "s3://" ++ DestinationBucket ++ "/" ++ DestinationKey}
      catch
        Error:Reason ->
          lager:error("cp (~ts -> s3://~ts/~ts) error: ~p:~p~n~p", [Source, DestinationBucket, DestinationKey, Error, Reason, erlang:get_stacktrace()]),
          {error, Reason}
      end;
    FSError ->
      FSError
  end.

copy_file_s3_to_s3(SourceBucket, SourceKey, DestinationBucket, DestinationKey, Options, AwsConfig) ->
  lager:info("COPY s3://~ts/~ts TO s3://~ts/~ts", [SourceBucket, SourceKey, DestinationBucket, DestinationKey]),
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
      lager:error("cp (s3://~ts/~ts -> s3://~ts/~ts) error: ~p:~p~n~p", [SourceBucket, SourceKey, DestinationBucket, DestinationKey, Error, Reason, erlang:get_stacktrace()]),
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
      lager:error("cp (s3://~ts/~ts -> s3://~ts/~ts) error: ~p:~p~n~p", [SourceBucket, SourceKey, DestinationBucket, DestinationKey, Error, Reason, erlang:get_stacktrace()]),
      {error, Reason}
  end.

copy_recursive_s3_to_s3(SourceBucket, SourceKey, DestinationBucket, DestinationKey, Options, AwsConfig) ->
  {
   Directories,
   Files
  } = case maps:get(copy_diff_only, Options, false) of
        false ->
          list_objects(SourceBucket, SourceKey, AwsConfig);
        true ->
          {SrcDirs, SrcFiles} = list_objects_with_size(SourceBucket, SourceKey, AwsConfig),
          {DestDirs, DestFiles} = list_objects_with_size(DestinationBucket, DestinationKey, AwsConfig),
          MissDirs = get_miss([{DestDirs, SrcDirs}]),
          MissFiles = get_miss([{DestFiles, SrcFiles}]),
          DifferFiles = get_differ(SrcFiles, DestFiles),
          {MissDirs, MissFiles ++ DifferFiles}
      end,
  case copy_objects_s3_to_s3(
         Files,
         SourceBucket,
         SourceKey,
         DestinationBucket,
         DestinationKey,
         Options,
         AwsConfig,
         copy_file_s3_to_s3) of
    ok ->
      case copy_objects_s3_to_s3(
             Directories,
             SourceBucket,
             SourceKey,
             DestinationBucket,
             DestinationKey,
             Options,
             AwsConfig,
             copy_directory_s3_to_s3) of
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
       erlcloud_s3:new(bucs:to_string(AccessKeyID), bucs:to_string(SecretAccessKey));
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

get_response_files(AwsResponse, AwsPrefix) ->
  get_response_elements(AwsResponse, contents, key, AwsPrefix).
get_response_files_with_size(AwsResponse, AwsPrefix) ->
  lists:foldl(fun({_, _} = File, Acc) -> [File|Acc];
                 (_, Acc) -> Acc
              end, [], get_response_elements(AwsResponse, contents, [key, size], AwsPrefix)).
get_response_directories(AwsResponse, AwsPrefix) ->
  get_response_elements(AwsResponse, common_prefixes, prefix, AwsPrefix).
get_response_elements(AwsResponse, Global, Local, AwsPrefix) ->
  lager:debug("Get response elements from ~p", [AwsResponse]),
  case lists:keyfind(Global, 1, AwsResponse) of
    false ->
      [];
    {Global, Prefixes} ->
      lists:foldr(fun(Prefix, Acc) ->
                      case is_list(Local) of
                        true ->
                          [list_to_tuple(
                             lists:foldr(fun(L, Acc0) ->
                                             case get_local(Prefix, L, AwsPrefix) of
                                               {true, Elem} -> [Elem|Acc0];
                                               false -> Acc0
                                             end
                                         end, [], Local))
                           |Acc];
                        false ->
                          case get_local(Prefix, Local, AwsPrefix) of
                            {true, Elem} -> [Elem|Acc];
                            false -> Acc
                          end
                      end
                  end, [], Prefixes)
  end.

get_local(Prefix, Local, AwsPrefix) ->
  case lists:keyfind(Local, 1, Prefix) of
    {Local, Data} when is_list(Data) ->
      case re:replace(Data, AwsPrefix, "", [{return, list}]) of
        "" -> false;
        Elem -> {true, Elem}
      end;
    {Local, Data} ->
      {true, Data};
    false ->
      false
  end.

delete_s3_file(Bucket, Key, AwsConfig) ->
  lager:debug("DELETE s3://~ts/~ts", [Bucket, Key]),
  erlcloud_s3:delete_object(Bucket, Key, AwsConfig),
  {ok, "s3://" ++ Bucket ++ "/" ++ Key}.

delete_s3_directory(Bucket, Key, Options, AwsConfig) ->
  Recursive = maps:get(recursive, Options, false),
  {Prefixes, Keys} = list_objects(Bucket, Key, AwsConfig),
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

list_objects(Bucket, Prefix, AwsConfig) ->
  list_objects(Bucket, Prefix, undefined, AwsConfig, fun get_response_directories/2, fun get_response_files/2, [], []).

list_objects_with_size(Bucket, Prefix, AwsConfig) ->
  list_objects(Bucket, Prefix, undefined, AwsConfig, fun get_response_directories/2, fun get_response_files_with_size/2, [], []).

list_objects(_Bucket, _Prefixes, [], _AwsConfig, _GetDirectories, _GetFiles, Directories, Files) ->
  {Directories, Files};
list_objects(Bucket, Prefix, Marker, AwsConfig, GetDirectories, GetFiles, Directories, Files) ->
  Response = erlcloud_s3:list_objects(
               Bucket,
               [{delimiter, "/"}, {prefix, Prefix} | marker(Marker)],
               AwsConfig),
  list_objects(
    Bucket,
    Prefix,
    proplists:get_value(next_marker, Response, []),
    AwsConfig,
    GetDirectories,
    GetFiles,
    Directories ++ erlang:apply(GetDirectories, [Response, Prefix]),
    Files ++ erlang:apply(GetFiles, [Response, Prefix])).

marker(undefined) -> [];
marker(Marker) -> [{marker, Marker}].
