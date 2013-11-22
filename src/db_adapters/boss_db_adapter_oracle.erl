-module(boss_db_adapter_oracle).
-behaviour(boss_db_adapter).
-export([init/1, terminate/1, start/1, stop/0, find/2, find/7]).
-export([count/3, counter/2, incr/3, delete/2, save_record/2]).
-export([push/2, pop/2, dump/1, execute/2, transaction/2]).
-export([get_migrations_table/1, migration_done/3]).

start(_) ->
    ok.

stop() ->
    ok.

init(Options) ->
  DBUsername = proplists:get_value(db_username, Options, "system"),
  DBPassword = proplists:get_value(db_password, Options, ""),
  DBTns = proplists:get_value(db_tns, Options, ""),
  application:start(erloci),
	OciPort = oci_port:start_link([{logging, true}]),
	OciSession = OciPort:get_session(DBTns, DBUsername, DBPassword).

terminate(Pid) -> 
  exit(Pid, normal).

find(Pid, Id) when is_list(Id) ->
  %%{Type, TableName, IdColumn, TableId} = boss_sql_lib:infer_type_from_id(Id),
  %%Statement = OciSession:prep_sql(list_to_binary(["SELECT * FROM ", TableName, " WHERE ", IdColumn, " = ", pack_value(TableId)])),
  %%Res = Statement:exec_stmt(),
  %%Statement:close(),
  %%case Res of
  %%  {ok, Cols} ->
  %%  	ok;
  %%  {error, Cols} ->
  %%    {error}
  %%end.
  {Pid}.

find(Pid, Type, Conditions, Max, Skip, Sort, SortOrder) when is_atom(Type), is_list(Conditions), 
                                                              is_integer(Max) orelse Max =:= all, is_integer(Skip), 
                                                              is_atom(Sort), is_atom(SortOrder) ->
	{Pid}.                                                              

count(Pid, Type, Conditions) ->
	{Pid, Type, Conditions}.

counter(Pid, Id) when is_list(Id) ->
	{Pid, Id}.

incr(Pid, Id, Count) ->
	{Pid, Id, Count}.

delete(Pid, Id) when is_list(Id) ->
	{Pid, Id}.

save_record(Pid, Record) when is_tuple(Record) ->
	{Pid, Record}.

push(Pid, Depth) ->
	{Pid, Depth}.

pop(Pid, Depth) ->
	{Pid, Depth}.

dump(_Conn) -> {_Conn}.

execute(Pid, Commands) ->
	{Pid, Commands}.

transaction(Pid, TransactionFun) when is_function(TransactionFun) ->
	{Pid}.
    
get_migrations_table(Pid) ->
	{Pid}.

migration_done(Pid, Tag, up) ->
	{Pid, Tag, up}.
