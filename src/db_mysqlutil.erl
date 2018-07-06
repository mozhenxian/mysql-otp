%%%-------------------------------------------------------------------
%%% @author Administrator
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 19. 十二月 2014 14:29
%%%-------------------------------------------------------------------

-module(db_mysqlutil).

%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% --------------------------------------------------------------------
%% Exported Functions
%% --------------------------------------------------------------------
-export([
    make_insert_sql/2,
    make_insert_sql/3,
    make_batch_insert_sql/3,

    make_select_sql/3,
    make_select_sql/5,

    make_replace_sql/3,
    make_replace_sql/2,
    make_batch_replace_sql/3,

    make_update_sql/3,
    make_update_sql/4,
    make_update_sql/5,

    make_delete_sql/2,
    make_create_sql/2,

    make_value_sql/2,
    make_filed_sql/2,
    make_batch_sql/2,
    make_batch_update/3,

    resolve_sql_file/1
]).

%% ====================================================================
%% API Functions
%% ====================================================================
-spec make_insert_sql(atom(), list(), list()) -> list().
%% @doc Make insert sql sentence.
make_insert_sql(Table_name, FieldList, ValueList) ->
    L = make_conn_sql(FieldList, ValueList, []),
    lists:concat(["insert into `", Table_name, "` set ", L]).

-spec make_insert_sql(atom(), list()) -> list().
%% @doc Make insert sql sentence.
make_insert_sql(Table_name, Field_Value_List) ->
    Fun = fun({Field, Val}, Sum) ->
        Expr = case is_binary(Val) orelse is_list(Val) of
                   true ->
                       io_lib:format("`~s`='~s'", [Field, re:replace(Val, "(['\\\\])", "&&", [global, {return, binary}])]);
                   _ -> io_lib:format("`~s`=~p", [Field, Val])
               end,
        S1 = if Sum == length(Field_Value_List) -> io_lib:format("~s", [Expr]);
                 true -> io_lib:format("~s,", [Expr])
             end,
        {S1, Sum + 1}
          end,
    {Vsql, _Count1} = lists:mapfoldl(Fun, 1, Field_Value_List),
    lists:concat(["insert into `", Table_name, "` set ",
        lists:flatten(Vsql)
    ]).

-spec make_batch_insert_sql(atom(), list(), list()) -> list().
make_batch_insert_sql(Table_name, FieldList,ValueList) ->
    Fields = lists:concat(["(",string:join([io_lib:format("`~s`", [Field])||Field<-FieldList],","),")"]),
    ValueFun = fun(Val, Sum) ->
        Expr = case is_binary(Val) orelse is_list(Val) of
                   true ->
                       io_lib:format("'~s'", [Val]);
                   _ -> io_lib:format("~p", [ Val])
               end,
        S1 = if Sum == length(FieldList) -> io_lib:format("~s", [Expr]);
                 true -> io_lib:format("~s,", [Expr])
             end,
        {S1, Sum + 1}
               end,
    Values = string:join([begin
                              {Vsql, _Count1} = lists:mapfoldl(ValueFun, 1, ValueList0),
                              lists:concat(["(",Vsql,")"])
                          end||ValueList0<-ValueList],","),

    lists:concat(["insert into `", Table_name,"`",Fields, " values ",
        Values
    ]).

-spec make_replace_sql(atom(), list(), list()) -> list().
%% @doc Make replace sql sentence.
make_replace_sql(Table_name, FieldList, ValueList) ->
    L = make_conn_sql(FieldList, ValueList, []),
    lists:concat(["replace into `", Table_name, "` set ", L]).

-spec make_replace_sql(atom(), list()) -> list().
%% @doc Make replace sql sentence.
make_replace_sql(Table_name, Field_Value_List) ->
    Fun = fun({Field, Val}, Sum) ->
        Expr = case is_binary(Val) orelse is_list(Val) of
                   true ->
                       io_lib:format("`~s`='~s'", [Field, re:replace(Val, "(['\\\\])", "&&", [global, {return, binary}])]);
                   _ ->
                       io_lib:format("`~s`=~p", [Field, Val])
               end,
        S1 = if Sum == length(Field_Value_List) -> io_lib:format("~s", [Expr]);
                 true -> io_lib:format("~s,", [Expr])
             end,
        {S1, Sum + 1}
          end,
    {Vsql, _Count1} = lists:mapfoldl(Fun, 1, Field_Value_List),
    lists:concat(["replace into `", Table_name, "` set ",
        lists:flatten(Vsql)
    ]).

-spec make_batch_replace_sql(atom(), list(), list()) -> list().
make_batch_replace_sql(Table_name, FieldList,ValueList) ->
    Fields = lists:concat(["(",string:join([io_lib:format("`~s`", [Field])||Field<-FieldList],","),")"]),
    ValueFun = fun(Val, Sum) ->
        Expr = case is_binary(Val) orelse is_list(Val) of
                   true ->
                       io_lib:format("'~s'", [Val]);
                   _ -> io_lib:format("~p", [ Val])
               end,
        S1 = if Sum == length(FieldList) -> io_lib:format("~s", [Expr]);
                 true -> io_lib:format("~s,", [Expr])
             end,
        {S1, Sum + 1}
               end,
    Values = string:join([begin
                              {Vsql, _Count1} = lists:mapfoldl(ValueFun, 1, ValueList0),
                              lists:concat(["(",Vsql,")"])
                          end||ValueList0<-ValueList],","),

    lists:concat(["replace into `", Table_name,"`",Fields, " values ",
        Values
    ]).

%make_replace_sql_batch(Table_name, FieldList, ValueList) -> ok.

-spec make_update_sql(atom(), list(), list(), term(), term()) -> list().
%% @doc Make update sql sentence.
make_update_sql(Table_name, Field, Data, Key, Value) ->
    L = make_conn_sql(Field, Data, []),
    lists:concat(["update `", Table_name, "` set ", L, " where `", Key, "`='", to_list(Value), "'"]).

%% @doc Make update sql sentence.
make_update_sql(Table_name, Field, Data, Where_List) ->
    L = make_conn_sql(Field, Data, []),
    WL = make_where_sql(Where_List),
    lists:concat(["update `", Table_name, "` set ", L, WL, " "]).

-spec make_update_sql(atom(), list(), list()) -> list().
%% @doc Make update sql sentence.
make_update_sql(Table_name, Field_Value_List, Where_List) ->
    Fun = fun(Field_value, Sum) ->
        Expr = case Field_value of
                   {Field, Val, add} -> io_lib:format("`~s`=`~s`+~p", [Field, Field, Val]);
                   {Field, Val, sub} -> io_lib:format("`~s`=`~s`-~p", [Field, Field, Val]);
                   {Field, Val} ->
                       case is_binary(Val) orelse is_list(Val) of
                           true ->
                               io_lib:format("`~s`='~s'", [Field, re:replace(Val, "(['\\\\])", "&&", [global, {return, binary}])]);
                           _ -> io_lib:format("`~s`=~p", [Field, Val])
                       end
               end,
        S1 = if Sum == length(Field_Value_List) -> io_lib:format("~s", [Expr]);
                 true -> io_lib:format("~s,", [Expr])
             end,
        {S1, Sum + 1}
          end,
    {Vsql, _Count1} = lists:mapfoldl(Fun, 1, Field_Value_List),
    {Wsql, Count2} = get_where_sql(Where_List),
    WhereSql =
        if Count2 > 1 -> lists:concat([" where ", lists:flatten(Wsql)]);
            true -> ""
        end,
    lists:concat(["update `", Table_name, "` set ",
        lists:flatten(Vsql), WhereSql, ""
    ]).

-spec make_delete_sql(atom(), list()) -> list().
%% @doc Make delete sql sentence.
make_delete_sql(Table_name, Where_List) ->
    {Wsql, Count2} = get_where_sql(Where_List),
    WhereSql =
        if Count2 > 1 -> lists:concat(["where ", lists:flatten(Wsql)]);
            true -> ""
        end,
    lists:concat(["delete from `", Table_name, "` ", WhereSql]).

-spec make_select_sql(atom(), string(), list()) -> list().
%% @doc Make select sql sentence.
make_select_sql(Table_name, Fields_sql, Where_List) ->
    make_select_sql(Table_name, Fields_sql, Where_List, [], []).

-spec make_select_sql(atom(), string(), list(), list(), list()) -> list().
%% @doc Make select sql sentence.
make_select_sql(Table_name, Fields_sql, Where_List, Order_List, Limit_num) ->
    {Wsql, Count1} = get_where_sql(Where_List),
    WhereSql =
        if Count1 > 1 -> lists:concat(["where ", lists:flatten(Wsql)]);
            true -> ""
        end,
    {Osql, Count2} = get_order_sql(Order_List),
    OrderSql =
        if Count2 > 1 -> lists:concat([" order by ", lists:flatten(Osql)]);
            true -> ""
        end,
    LimitSql = case Limit_num of
                   [] -> "";
                   [Num] -> lists:concat([" limit ", Num])
               end,
    lists:concat(["select ", Fields_sql, " from `", Table_name, "` ", WhereSql, OrderSql, LimitSql]).


make_value_sql([], L) ->
    L;
make_value_sql([D | T], []) ->
    L = ["('", get_sql_val(D), "'"],
    make_value_sql(T, L);
make_value_sql([D | T], L) ->
    L1 = case T of
             [] ->
                 L ++ [",'", get_sql_val(D), "')"];
             _ ->
                 L ++ [",'", get_sql_val(D), "'"]
         end,
    make_value_sql(T, L1).


make_filed_sql([], L) ->
    L;
make_filed_sql([D | T], []) ->
    make_filed_sql(T, lists:concat(["(`", D]));
make_filed_sql([D | T], L) ->
    case T of
        [] ->
            make_filed_sql(T, lists:concat([L, "`,`", D, "`)"]));
        _ ->
            make_filed_sql(T, lists:concat([L, "`,`", D]))
    end.

make_batch_sql([], L) ->
    L;
make_batch_sql([D | T], L) ->
    L1 = case T of
             [] ->
                 L ++ [make_value_sql(D, []), ";"];
             _ ->
                 L ++ [make_value_sql(D, []), ","]
         end,
    make_batch_sql(T, L1).

make_batch_update([], L, _Update) ->
    L;
make_batch_update([D | T], L, Update) ->
    L1 = case T of
             [] ->
                 L ++ [make_value_sql(D, []), " ", Update, ";"];
             _ ->
                 L ++ [make_value_sql(D, []), ","]
         end,
    make_batch_update(T, L1, Update).


resolve_sql_file(File) ->
    case file:read_file(File) of
        {ok, Sql} ->
            SqlList = string:tokens(binary_to_list(Sql), ";"),
            CmdList = lists:foldl(
                fun(Cmd, Acc) ->
                    NewCmd = util:replace(Cmd, [{"\r\n", ""}]),
                    case NewCmd of
                        [] ->
                            Acc;
                        _ ->
                            [NewCmd | Acc]
                    end
                end, [], SqlList),
            lists:reverse(CmdList);
        Error ->
            io:format("update sql file:~p  error ~p", [File, Error]),
            []
    end.

%% ====================================================================
%% Local Functions
%% ====================================================================
make_conn_sql([], _, L) ->
    L;
make_conn_sql(_, [], L) ->
    L;
make_conn_sql([F | T1], [D | T2], []) ->
    L = ["`", to_list(F), "`='", get_sql_val(D), "'"],
    make_conn_sql(T1, T2, L);
make_conn_sql([F | T1], [D | T2], L) ->
    L1 = L ++ [",`", to_list(F), "`='", get_sql_val(D), "'"],
    make_conn_sql(T1, T2, L1).

make_where_sql(Where_List) ->
    {Wsql, Count2} = get_where_sql(Where_List),
    if Count2 > 1 -> lists:concat([" where ", lists:flatten(Wsql)]);
        true -> ""
    end.

get_sql_val(Val) ->
    case is_binary(Val) orelse is_list(Val) of
        true -> re:replace(to_list(Val), "(['\\\\])", "&&", [global, {return, list}]);
        _ -> to_list(Val)
    end.

get_order_sql(Order_List) ->
    Fun = fun(Field_Order, Sum) ->
        Expr =
            case Field_Order of
                {Field, Order} ->
                    io_lib:format("~p ~p", [Field, Order]);
                {Field} ->
                    io_lib:format("~p", [Field]);
                _ -> ""
            end,
        S1 = if Sum == length(Order_List) -> io_lib:format("~s", [Expr]);
                 true -> io_lib:format("~s,", [Expr])
             end,
        {S1, Sum + 1}
          end,
    lists:mapfoldl(Fun, 1, Order_List).

get_where_sql(Where_List) ->
    Fun = fun(Field_Operator_Val, Sum) ->
        [Expr, Or_And_1] =
            case Field_Operator_Val of
                {Field, Operator, Val, Or_And} ->
                    case is_binary(Val) orelse is_list(Val) of
                        true ->
                            [io_lib:format("`~s`~s'~s'", [Field, Operator, re:replace(Val, "(['\\\\])", "&&", [global, {return, binary}])]), Or_And];
                        _ -> [io_lib:format("`~s`~s~p", [Field, Operator, Val]), Or_And]
                    end;
                {Field, in, Val} ->
                    [io_lib:format("`~s` IN ~s", [Field, re:replace(Val, "(['\\\\])", "&&", [global, {return, binary}])]), "and"];
                {Field, notin, Val} ->
                    [io_lib:format("`~s` NOT IN ~s", [Field, re:replace(Val, "(['\\\\])", "&&", [global, {return, binary}])]), "and"];
                {Field, Operator, Val} ->
                    case is_binary(Val) orelse is_list(Val) of
                        true ->
                            [io_lib:format("`~s`~s'~s'", [Field, Operator, re:replace(Val, "(['\\\\])", "&&", [global, {return, binary}])]), "and"];
                        _ -> [io_lib:format("`~s`~s~p", [Field, Operator, Val]), "and"]
                    end;
                {Field, Val} ->
                    case is_binary(Val) orelse is_list(Val) of
                        true ->
                            [io_lib:format("`~s`='~s'", [Field, re:replace(Val, "(['\\\\])", "&&", [global, {return, binary}])]), "and"];
                        _ -> [io_lib:format("`~s`=~p", [Field, Val]), "and"]
                    end;
                _ ->
                    ["true", "and"]
            end,
        S1 = if Sum == length(Where_List) -> io_lib:format("~s", [Expr]);
                 true -> io_lib:format("~s ~s ", [Expr, Or_And_1])
             end,
        {S1, Sum + 1}
          end,
    lists:mapfoldl(Fun, 1, Where_List).

-spec make_create_sql(atom(), list()) -> list().
%% @doc Make insert sql sentence.
make_create_sql(Table_name, Field_List) ->
    %%?WARNING_MSG("Field_List ~p",[Field_List]),
    DropSql = lists:concat(["DROP TABLE IF EXISTS `", Table_name, "`;\n"]),
    Fun = fun(X, {Count, Sql}) ->
        %%?WARNING_MSG("CCC ~p",[X]),
        NewSql = case Count =:= length(Field_List) of
                     true ->
                         lists:concat([Sql, X, " varchar(50) DEFAULT '' \n"]);
                     _ ->
                         lists:concat([Sql, X, " varchar(50) DEFAULT '',\n"])
                 end,
        {Count + 1, NewSql}
          end,
    {_, ValSql} = lists:foldl(Fun, {1, ""}, Field_List),
    lists:concat([DropSql, "CREATE TABLE `", Table_name, "` ( ", ValSql, " ) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;\n"]).

to_list(Val) when is_list(Val) -> Val;
to_list(Val) when is_atom(Val) -> atom_to_list(Val);
to_list(Val) when is_binary(Val) -> binary_to_list(Val);
to_list(Val) when is_integer(Val) -> integer_to_list(Val);
to_list(Val) when is_tuple(Val) -> tuple_to_list(Val);
to_list(_) -> throw(other_value).
%% ====================================================================
%% Test
%% ====================================================================
-ifdef(TEST).
make_sql_test() ->
    Table_name = t_players,

    Sql_1 = "insert into `t_players` set `id`='1',`name`='test'",
    Make_1 = make_insert_sql(Table_name, [id, name], [1, "test"]),
    ?assertEqual(Sql_1, lists:flatten(Make_1)),

    Sql_2 = "insert into `t_players` set `id`=1,`name`='test'",
    Make_2 = make_insert_sql(Table_name, [{id, 1}, {name, "test"}]),
    ?assertEqual(Sql_2, lists:flatten(Make_2)),

    Sql_3 = "replace into `t_players` set `id`=1,`name`='test'",
    Make_3 = make_replace_sql(Table_name, [{id, 1}, {name, "test"}]),
    ?assertEqual(Sql_3, lists:flatten(Make_3)),

    Sql_4 = "update `t_players` set `id`=`id`+1,`exp`=`exp`-1,`name`='test' where `name`='test'",
    Make_4 = make_update_sql(Table_name, [{id, 1, add}, {exp, 1, sub}, {name, "test"}], [{name, "=", "test"}]),
    ?assertEqual(Sql_4, lists:flatten(Make_4)),

    Sql_5 = "update `t_players` set `id`='1',`name`='test' where `id`='1'",
    Make_5 = make_update_sql(Table_name, [id, name], [1, "test"], id, 1),
    ?assertEqual(Sql_5, lists:flatten(Make_5)),

    Sql_6 = "delete from `t_players` where `id`>1 or `name`='test'",
    Make_6 = make_delete_sql(Table_name, [{id, ">", 1, "or"}, {name, "test"}]),
    ?assertEqual(Sql_6, lists:flatten(Make_6)),

    Sql_7 = "select id, name from `t_players` where `id`>1 and true",
    Make_7 = make_select_sql(Table_name, "id, name", [{id, ">", 1}, {skip}]),
    ?assertEqual(Sql_7, lists:flatten(Make_7)),

    Sql_8 = "select * from `t_players` where `name`='test' or `id`=1 order by id desc limit 1",
    Make_8 = make_select_sql(Table_name, "*", [{name, "=", "test", "or"}, {id, 1}], [{id, desc}], [1]),
    ?assertEqual(Sql_8, lists:flatten(Make_8)),

    Sql_9 = "replace into `t_players` set `id`='1',`name`='test'",
    Make_9 = make_replace_sql(Table_name, [id, name], [1, "test"]),
    ?assertEqual(Sql_9, lists:flatten(Make_9)),
    ok.
-endif.
