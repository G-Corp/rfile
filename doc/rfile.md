

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


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#cp-3">cp/3</a></td><td></td></tr><tr><td valign="top"><a href="#ls-2">ls/2</a></td><td></td></tr><tr><td valign="top"><a href="#rm-2">rm/2</a></td><td></td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="cp-3"></a>

### cp/3 ###

<pre><code>
cp(Source::string() | binary(), Destination::string() | binary(), Options::<a href="#type-options">options()</a>) -&gt; {ok, pid()} | {error, term()}
</code></pre>
<br />

<a name="ls-2"></a>

### ls/2 ###

<pre><code>
ls(Source::string() | binary(), Options::<a href="#type-options">options()</a>) -&gt; {ok, pid()} | {error, term()}
</code></pre>
<br />

<a name="rm-2"></a>

### rm/2 ###

<pre><code>
rm(Source::string() | binary(), Options::<a href="#type-options">options()</a>) -&gt; {ok, pid()} | {error, term()}
</code></pre>
<br />

