-module(rfile_aws_tests).
-include_lib("eunit/include/eunit.hrl").

rfile_aws_ls_test_() ->
  {setup,
   fun() ->
       meck:new(erlcloud_s3, [passthrough]),
       meck:expect(erlcloud_s3, list_objects,
                   fun(_, _, _) ->
                       [{common_prefixes, [[{prefix, "dir1/"}],
                                           [{prefix, "dir2/"}]]},
                        {contents, [[{key, "file1.txt"}],
                                    [{key, "file2.txt"}]]}]
                   end),
       ok
   end,
   fun(_) ->
       meck:unload(erlcloud_s3),
       ok
   end,
   [
    fun() ->
        ?assertMatch({ok, #{directories := ["dir1/", "dir2/"],
                            files := ["file1.txt", "file2.txt"]}},
                    rfile_aws:ls(#{file => "s3://test-bucket/", host => "test-bucket", path => ""}, #{}))
    end
   ]}.
