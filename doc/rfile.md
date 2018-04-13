

# Module rfile #
* [Data Types](#types)
* [Function Index](#index)
* [Function Details](#functions)

<a name="types"></a>

## Data Types ##




### <a name="type-acl">acl()</a> ###


<pre><code>
acl() = private | public_read | public_read_write | authenticated_read | bucket_owner_read | bucket_owner_full_control
</code></pre>




### <a name="type-aws">aws()</a> ###


<pre><code>
aws() = #{access_key_id =&gt; string(), secret_access_key =&gt; string()}
</code></pre>




### <a name="type-options">options()</a> ###


<pre><code>
options() = <a href="#type-options_map">options_map()</a> | <a href="#type-options_list">options_list()</a>
</code></pre>




### <a name="type-options_list">options_list()</a> ###


<pre><code>
options_list() = [{acl, <a href="#type-acl">acl()</a>} | {recursive, true | false} | {metadata, term()} | {callback, fun((atom(), [string() | binary()], {ok | error, term()}, term() | undefined) -&gt; ok)} | {aws, <a href="#type-aws">aws()</a>} | {source, [{aws, <a href="#type-aws">aws()</a>}]} | {destination, [{aws, <a href="#type-aws">aws()</a>}]}]
</code></pre>




### <a name="type-options_map">options_map()</a> ###


<pre><code>
options_map() = #{acl =&gt; <a href="#type-acl">acl()</a>, recursive =&gt; true | false, metadata =&gt; term(), callback =&gt; fun((atom(), [string() | binary()], {ok | error, term()}, term() | undefined) -&gt; ok), aws =&gt; <a href="#type-aws">aws()</a>, source =&gt; #{aws =&gt; <a href="#type-aws">aws()</a>}, destination =&gt; #{aws =&gt; <a href="#type-aws">aws()</a>}}
</code></pre>

<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#cp-3">cp/3</a></td><td>
Copy files and directories.</td></tr><tr><td valign="top"><a href="#jobs-0">jobs/0</a></td><td>
Return the number of queued jobs.</td></tr><tr><td valign="top"><a href="#ls-2">ls/2</a></td><td>
List directory content.</td></tr><tr><td valign="top"><a href="#max_jobs-1">max_jobs/1</a></td><td>
Update the maximum number of jobs.</td></tr><tr><td valign="top"><a href="#rm-2">rm/2</a></td><td>
Remove files or directories.</td></tr><tr><td valign="top"><a href="#status-1">status/1</a></td><td>
Return the status of a given job.</td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="cp-3"></a>

### cp/3 ###

<pre><code>
cp(Source::string() | binary(), Destination::string() | binary(), Options::<a href="#type-options">options()</a>) -&gt; {ok, reference()} | {error, term()}
</code></pre>
<br />

Copy files and directories

<a name="jobs-0"></a>

### jobs/0 ###

<pre><code>
jobs() -&gt; integer()
</code></pre>
<br />

Return the number of queued jobs.

<a name="ls-2"></a>

### ls/2 ###

<pre><code>
ls(Source::string() | binary(), Options::<a href="#type-options">options()</a>) -&gt; {ok, reference()} | {error, term()}
</code></pre>
<br />

List directory content

<a name="max_jobs-1"></a>

### max_jobs/1 ###

<pre><code>
max_jobs(Max::integer()) -&gt; ok
</code></pre>
<br />

Update the maximum number of jobs.

<a name="rm-2"></a>

### rm/2 ###

<pre><code>
rm(Source::string() | binary(), Options::<a href="#type-options">options()</a>) -&gt; {ok, reference()} | {error, term()}
</code></pre>
<br />

Remove files or directories

<a name="status-1"></a>

### status/1 ###

<pre><code>
status(Job::reference()) -&gt; queued | started | terminated
</code></pre>
<br />

Return the status of a given job.

