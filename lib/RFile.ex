# File: RFile.ex
# This file was generated from rfile.beam
# Using rebar3_elixir (https://github.com/G-Corp/rebar3_elixir)
# MODIFY IT AT YOUR OWN RISK AND ONLY IF YOU KNOW WHAT YOU ARE DOING!
defmodule RFile do
  def unquote(:"max_jobs")(arg1) do
    :erlang.apply(:"rfile", :"max_jobs", [arg1])
  end
  def unquote(:"jobs")() do
    :erlang.apply(:"rfile", :"jobs", [])
  end
  def unquote(:"status")(arg1) do
    :erlang.apply(:"rfile", :"status", [arg1])
  end
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
