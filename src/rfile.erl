-module(rfile).
% -compile([{parse_transform, lager_transform}]).

-export([
         ls/2
         , cp/3
         , rm/2

         , max_jobs/1
         , jobs/0
         , status/1
        ]).

-export_type([
              options/0
             ]).

-define(FILE_REGEX, "((?<scheme>[^:]*):\/\/)?((?<host>(\/[^\/]*|[^\/]*))\/)?(?<path>.*)").
-define(PROVIDERS_CONFIG, [{{aws, aws}, rfile_aws},
                           {{aws, fs}, rfile_aws},
                           {{fs, aws}, rfile_aws},
                           {aws, rfile_aws}]).

-type aws() :: #{
        access_key_id => string(),
        secret_access_key => string()
       }.

-type acl() :: private | public_read | public_read_write | authenticated_read | bucket_owner_read | bucket_owner_full_control.

-type options_map() :: #{% AWS
        acl => acl(),
        % common
        recursive => true | false,
        metadata => term(),
        callback => fun((atom(), [string() | binary()], {ok | error, term()}, term() | undefined) -> ok),
                      % Auth
                      aws => aws(),
                      source => #{
                        aws => aws()
                       },
                      destination => #{
                        aws => aws()
                       }
                    }.

-type options_list() :: [
                         {acl, acl()}
                         | {recursive, true | false}
                         | {metadata, term()}
                         | {callback, fun((atom(), [string() | binary()], {ok | error, term()}, term() | undefined) -> ok)}
                         | {aws, aws()}
                         | {source, [{aws, aws()}]}
                         | {destination, [{aws, aws()}]}
                        ].

-type options() :: options_map() | options_list().

% @doc
% Update the maximum number of jobs.
% @end
-spec max_jobs(Max::integer()) -> ok.
max_jobs(Max) when is_integer(Max) ->
  gen_server:cast(rfile_workers_queue, {max_jobs, Max}).

% @doc
% Return the number of queued jobs.
% @end
-spec jobs() -> integer().
jobs() ->
  gen_server:call(rfile_workers_queue, jobs).

% @doc
% Return the status of a given job.
% @end
-spec status(Job::reference()) -> queued | started | terminated.
status(Job) when is_reference(Job) ->
  gen_server:call(rfile_workers_queue, {status, Job}).

% @doc
% List directory content
% @end
-spec ls(Source::string() | binary(),
         Options::options()) -> {ok, reference()} | {error, term()}.
ls(Source, Options) ->
  case find_provider(Source) of
    {error, _Reason} = Error ->
      Error;
    Other ->
      Ref = erlang:make_ref(),
      gen_server:cast(rfile_workers_queue, {{ls, Other, Options}, Ref}),
      {ok, Ref}
  end.

% @doc
% Copy files and directories
% @end
-spec cp(Source::string() | binary(),
         Destination::string() | binary(),
         Options::options()) -> {ok, reference()} | {error, term()}.
cp(Source, Destination, Options) ->
  case find_provider(Source, Destination) of
    {error, _Reason} = Error ->
      Error;
    Other ->
      Ref = erlang:make_ref(),
      gen_server:cast(rfile_workers_queue, {{cp, Other, Options}, Ref}),
      {ok, Ref}
  end.

% @doc
% Remove files or directories
% @end
-spec rm(Source::string() | binary(),
         Options::options()) -> {ok, reference()} | {error, term()}.
rm(Source, Options) ->
  case find_provider(Source) of
    {error, _Reason} = Error ->
      Error;
    Other ->
      Ref = erlang:make_ref(),
      gen_server:cast(rfile_workers_queue, {{rm, Other, Options}, Ref}),
      {ok, Ref}
  end.

find_provider(Source) ->
  case cut(Source) of
    #{type := T} = Src ->
      case get_provider(T) of
        {ok, Provider} ->
          #{provider => Provider,
            source => Src};
        Error ->
          Error
      end;
    Error ->
      Error
  end.

find_provider(Source, Destination) ->
  case {cut(Source), cut(Destination)} of
    {#{type := T1} = Src, #{type := T2} = Dest} ->
      case get_provider({T1, T2}) of
        {ok, Provider} ->
          #{provider => Provider,
            source => Src,
            destination => Dest};
        Error ->
          Error
      end;
    {{error, _Reason} = Error, #{}} ->
      Error;
    {#{}, {error, _Reason} = Error} ->
      Error;
    {_Src, _Dest} ->
      {error, unsupported}
  end.

get_provider(Type) ->
  case lists:keyfind(Type, 1, ?PROVIDERS_CONFIG) of
    false ->
      {error, not_supported};
    {_, Provider} ->
      {ok, Provider}
  end.

cut(File) ->
  case re:run(File, ?FILE_REGEX, [{capture, [scheme, host, path], list}]) of
    {match, [Scheme, Host, Path]} -> add_type(#{
                                       scheme => Scheme,
                                       host => Host,
                                       path => Path,
                                       file => File
                                      });
    _ ->
      {error, [invalid_file, File]}
  end.

add_type(#{scheme := "s3"} = File) ->
  File#{type => aws};
add_type(#{scheme := "file"} = File) ->
  File#{type => fs};
add_type(File) ->
  {error, {unsuported_provider, File}}.
