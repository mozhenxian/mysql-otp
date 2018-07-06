%%%-------------------------------------------------------------------
%%% @author cd
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 26. 六月 2018 20:43
%%%-------------------------------------------------------------------
-module(test_mysql_otp).
-author("cd").

%% API
-export([
    start/0
    ,get_some/0
    ,start_old/0
    ,init/1
    ,start_sup/0
]).

-behaviour(supervisor).

-define(GAME_POOL, game_pool).
-define(LOG_POOL,  log_pool).


start_sup() ->
%%    supervisor:start_link(?MODULE, []),
%%    supervisor:start_link({local, ?MODULE}, ?MODULE, []), %% 启动supervisor 在init中返回链接数据库参数
    mysql_poolboy_sup:start_link(),
%%    io:format("wheris = ~p~n", [whereis(?GAME_POOL)]),
    start(),
    receive
        ok -> ok
    end,
    ok.

start_old() ->
    {ok, Pid} = mysql:start_link([{host, "localhost"}, {user, "root"},
        {password, "xiaomo"}, {database, "qzsg_game"}]),
    Ret = mysql:query(Pid, "select * from t_sys_kv"),
    io:format("Ret = ~p~n", [Ret]),
    ok.

init([]) ->
    PoolOptions  = [{size, 10}, {max_overflow, 20}],
    MySqlOptions = [{user, "root"}, {password, "xiaomo"}, {database, "qzsg_game"}],
    io:format("Ret = ~p~n", [mo]),
    ChildSpecs = [
        %% MySQL pools
        mysql_poolboy:child_spec(?GAME_POOL, PoolOptions, MySqlOptions)
    ],
    {ok, {{one_for_one, 10, 10}, ChildSpecs}}.


start() ->
%%    Ret = mysql_poolboy:query(?GAME_POOL, "select * from t_sys_kv"),
%%    io:format("Ret = ~p~n", [Ret]),

    ComPoolOptions = [{size, 10}, {max_overflow, 20}],
    GameMySqlOptions = [{user, "root"}, {password, "xiaomo"}, {database, "qzsg_game"}],
    LogMySqlOptions = [{user, "root"}, {password, "xiaomo"}, {database, "qzsg_log"}],
    mysql_poolboy:add_pool(?GAME_POOL, ComPoolOptions, GameMySqlOptions),
    mysql_poolboy:add_pool(?LOG_POOL, ComPoolOptions, LogMySqlOptions),

    Ret = mysql_poolboy:query(?GAME_POOL, "select * from t_sys_kv where skey = 48"),
    io:format("Ret = ~p~n", [Ret]),

    Ret2 = mysql_poolboy:query(?LOG_POOL, "select * from t_log_act where id = 1"),
    io:format("Ret2 = ~p~n", [Ret2]),

    ok.

get_some() ->
    Ret = mysql_poolboy:query(?GAME_POOL, "replace into t_sys_kv (skey, value) values ('11111', 1)"),
    io:format("Ret = ~p~n", [Ret]),

    Ret2 = mysql_poolboy:query(?GAME_POOL, "select * from t_sys_kv where skey = 48"),
    io:format("Ret = ~p~n", [Ret2]),

    Sql = "select version from t_version",
    Ret3 = mysql_poolboy:query(?GAME_POOL, Sql),
    io:format("Ret3 = ~p~n", [Ret3]),

    Ret4 = mysql_poolboy:query(?GAME_POOL, "select data from t_player_arena where player_id = 1"),
    io:format("Ret4 = ~p~n", [Ret4]),

    Term =  erlang:term_to_binary({a, b}, [{compressed, 5}]),

%%    Sql2 = lists:concat(["replace into t_sys_kv_bin (skey, value) values(", 78, ",'",
%%        erlang:binary_to_list(Term), "')"]),
    _Sql2 = db_mysqlutil:make_replace_sql(t_sys_kv_bin, [skey,value], [1001,Term]),
    Sql2 = "replace INTO t_log_union (union_id,union_name,union_lv,creator_name,captain_id,captain_name,member_num,create_time,announcement,time) VALUES ('8589934592145','3333','1','s2000.月寒故事','8589934592061','s2000.月寒故事','1','1530005529','欢迎加入本军团，一起努力。周一、三、五、日晚上9点军团战，不见不散。无兄弟，不军团。','1530863634');",
    io:format("Sql = ~p~n", [Sql2]),
%%    Worker = poolboy:checkout(?GAME_POOL, true, 5000),
%%    Ret5 = mysql:query(Worker, "replace into t_sys_kv_bin (skey, value) values(?, ?)", ["10988", Term]),
    Ret5 = mysql_poolboy:query(?GAME_POOL, Sql2, 5000),
    io:format("Ret5 = ~p~n", [Ret5]),



    ok.


