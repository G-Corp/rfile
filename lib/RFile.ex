# File: RFile.ex
# This file was generated from rfile.beam
# Using rebar3_elixir (https://github.com/botsunit/rebar3_elixir)
# MODIFY IT AT YOUR OWN RISK AND ONLY IF YOU KNOW WHAT YOU ARE DOING!
defmodule RFile do
  def unquote(:"ls")(arg1, arg2) do
    :erlang.apply(:"rfile", :"ls", [arg1, arg2])
  end
  def unquote(:"cp")(arg1, arg2, arg3) do
    :erlang.apply(:"rfile", :"cp", [arg1, arg2, arg3])
  end
  def unquote(:"rm")(arg1, arg2) do
    :erlang.apply(:"rfile", :"rm", [arg1, arg2])
  end
end
