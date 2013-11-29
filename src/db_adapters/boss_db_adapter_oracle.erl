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
	{OciPort, OciPort:get_session(DBTns, DBUsername, DBPassword)}.

terminate(Pid) -> 
  exit(Pid, normal).

find(Pid, Id) when is_list(Id) ->
  {_,OciSession} = Pid,
  {Type, TableName, IdColumn, TableId} = boss_sql_lib:infer_type_from_id(Id),
  Statement = OciSession:prep_sql(list_to_binary(["SELECT * FROM ", TableName, " WHERE ", IdColumn, " = ", pack_value(TableId)])),
  Res = Statement:exec_stmt(),
  Statement:close(),
  case Res of
    {ok, Cols} ->
    	ok;
    {error, Cols} ->
      {error}
  end.

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


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

add_cond(Acc, Key, Op, PackedVal) ->
    [lists:concat([Key, " ", Op, " ", PackedVal, " AND "])|Acc].

pack_match(Key) ->
    lists:concat(["MATCH(", Key, ")"]).

pack_match_not(Key) ->
    lists:concat(["NOT MATCH(", Key, ")"]).

pack_boolean_query(Values, Op) ->
    "('" ++ string:join(lists:map(fun(Val) -> Op ++ escape_sql(Val) end, Values), " ") ++ "' IN BOOLEAN MODE)".

pack_set(Values) ->
    "(" ++ string:join(lists:map(fun pack_value/1, Values), ", ") ++ ")".

pack_range(Min, Max) ->
    pack_value(Min) ++ " AND " ++ pack_value(Max).

escape_sql(Value) ->
    escape_sql1(Value, []).

escape_sql1([], Acc) ->
    lists:reverse(Acc);
escape_sql1([$'|Rest], Acc) ->
    escape_sql1(Rest, [$', $'|Acc]);
escape_sql1([C|Rest], Acc) ->
    escape_sql1(Rest, [C|Acc]).

pack_datetime(DateTime) ->
    "'" ++ erlydtl_filters:date(DateTime, "Y-m-d H:i:s") ++ "'".

pack_date(Date) ->
    "'" ++ erlydtl_filters:date(Date, "Y-m-d") ++ "'".

pack_now(Now) -> pack_datetime(calendar:now_to_datetime(Now)).

pack_value(null) ->
	"null";
pack_value(undefined) ->
	"null";
pack_value(V) when is_binary(V) ->
    pack_value(binary_to_list(V));
pack_value(V) when is_list(V) ->
    "'" ++ escape_sql(V) ++ "'";
pack_value({_, _, _} = Val) ->
	pack_date(Val);    
pack_value({{_, _, _}, {_, _, _}} = Val) ->
    pack_datetime(Val);
pack_value(Val) when is_integer(Val) ->
    integer_to_list(Val);
pack_value(Val) when is_float(Val) ->
    float_to_list(Val);
pack_value(true) ->
    "TRUE";
pack_value(false) ->
    "FALSE".