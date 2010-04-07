-module(chord_server).
-behaviour(gen_server).
-compile(export_all).

-include("../include/common_chord.hrl").

%% API
-export([start/0,
        start/1,
        server/0]).

%% gen_server callbacks
-export([init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3]).


%%====================================================================
%% API
%%====================================================================
start() ->
    start(nil).

start(InitNode) ->
    gen_server:start({local, ?MODULE}, ?MODULE, [InitNode], []).


server() ->
    {ok, whereis(?MODULE)}.


%%====================================================================
%% gen_server callbacks
%%====================================================================
%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([nil]) ->
    State = #state {
        myhash = myhash(),
        succlist = #succlist {
            bigger_len = 0,
            bigger = [],
            smaller = []
        }
    },
    {ok, State};
init([InitNode]) ->
    case net_adm:ping(InitNode) of
        pong -> {ok, init_successor_list(InitNode)};
        pang -> {error, {not_found, InitNode}}
    end.

init_successor_list(InitNode) ->
    MyHash = myhash(),
    State = #state {
        myhash = MyHash
    },
    case rpc:call(InitNode, ?MODULE, server, []) of
        {ok, InitServer} -> State#state{succlist=gen_server:call(InitServer, {join_op, MyHash})};
        {badrpc, Reason} -> {stop, Reason}
    end.


%%--------------------------------------------------------------------
%% Function: handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                                {reply, Reply, State, Timeout} |
%%                                                {noreply, State} |
%%                                                {noreply, State, Timeout} |
%%                                                {stop, Reason, Reply, State} |
%%                                                {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call({join_op, NewHash}, From, State) ->
    {noreply, State#state{succlist=join_op(NewHash, From, State#state.myhash, State#state.succlist)}};

handle_call(_, _, State) ->
    {noreply, State}.


%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast({join_op_cast, NewHash, From}, State) ->
    {noreply, State#state{succlist=join_op(NewHash, From, State#state.myhash, State#state.succlist)}};

handle_cast(_, State) ->
    {noreply, State}.


%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info(_, State) ->
    {noreply, State}.


%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_, _) ->
    ok.


%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_, State, _) ->
    {ok, State}.


%%====================================================================
%% called by handle_call/3
%%====================================================================
join_op(NewHash, From, MyHash, MySuccList) ->
    {S_or_B, SuccList, Len} = if
        NewHash > MyHash -> {bigger, MySuccList#succlist.bigger, MySuccList#succlist.bigger_len};
        NewHash < MyHash -> {smaller, MySuccList#succlist.smaller, ?LENGTH_SUCCESSOR_LIST - MySuccList#succlist.bigger_len}
    end,
    case next(S_or_B, NewHash, SuccList, Len) of
        self ->
            join_op_1(S_or_B, NewHash, From, MySuccList, MySuccList);
        shortage ->
            join_op_shortage(S_or_B, NewHash, From, MyHash, MySuccList);
        {error, Reason} ->
            gen_server:reply(From, {error, Reason}),
            MySuccList;
        {OtherNode, _} ->
            gen_server:cast(OtherNode, {join_op_cast, NewHash, From}),
            MySuccList
    end.

join_op_shortage(bigger, NewHash, From, MyHash, MySuccList) ->
    ResultSuccList = MySuccList#succlist {
        bigger_len = MySuccList#succlist.bigger_len + 1,
        bigger = MySuccList#succlist.bigger ++ [{whereis(?MODULE), MyHash}]
    },
    join_op_1(bigger, NewHash, From, MySuccList, ResultSuccList);
join_op_shortage(smaller, NewHash, From, MyHash, MySuccList) ->
    ResultSuccList = MySuccList#succlist {
        bigger_len = MySuccList#succlist.bigger_len - 1,
        smaller = MySuccList#succlist.smaller ++ [{whereis(?MODULE), MyHash}]
    },
    join_op_1(smaller, NewHash, From, MySuccList, ResultSuccList).

join_op_1(S_or_B, NewHash, From, MySuccList, ResultSuccList) ->
    gen_server:reply(From, ResultSuccList),
    join_op_2(S_or_B, NewHash, From, MySuccList).

join_op_2(bigger, NewHash, From, MySuccList) ->
    {NewServer, _} = From,
    MySuccList#succlist {
        bigger_len = MySuccList#succlist.bigger_len + 1,
        bigger = [{NewServer, NewHash} | MySuccList#succlist.bigger]
    };
join_op_2(smaller, NewHash, From, MySuccList) ->
    {NewServer, _} = From,
    MySuccList#succlist {
        bigger_len = MySuccList#succlist.bigger_len - 1,
        bigger = [{NewServer, NewHash} | MySuccList#succlist.bigger]
    }.


lookup_op() ->
    pass.


%%====================================================================
%% utilities
%%====================================================================
myhash() ->
    crypto:start(),
    crypto:sha_init(),
    crypto:sha(list_to_binary(atom_to_list(node()))).


next(_, NewHash, [{_, NewHash} | _], _) ->
    {error, equal_hash};
next(bigger, NewHash, [{_, Hash} | _], _) when NewHash > Hash ->
    self;
next(smaller, NewHash, [{_, Hash} | _], _) when NewHash < Hash ->
    self;
next(_, _, [], _) ->
    shortage;
next(S_or_B, NewHash, SuccList, Len) ->
    next_1(S_or_B, NewHash, SuccList, Len, 1).

next_1(_, _, [Peer | _], Len, Len) ->
    Peer;
next_1(_, _, [_Peer], Len, Count) when Count /= Len->
    shortage;

next_1(bigger, NewHash, [{Server, Hash} | _], _, _) when NewHash > Hash ->
    {Server, Hash};
next_1(bigger, NewHash, [{_, Hash} | Tail], Len, Count) when NewHash < Hash ->
    next_1(bigger, NewHash, Tail, Len, Count + 1);

next_1(smaller, NewHash, [{Server, Hash} | _], _, _) when NewHash < Hash ->
    {Server, Hash};
next_1(smaller, NewHash, [{_, Hash} | Tail], Len, Count) when NewHash > Hash ->
    next_1(smaller, NewHash, Tail, Len, Count + 1).
