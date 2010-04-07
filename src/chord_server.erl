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
        succlist = lists:duplicate(?LENGTH_SUCCESSOR_LIST, nil)
    },
    {ok, State};
init([InitNode]) ->
    case net_adm:ping(InitNode) of
        pong -> init_successor_list(InitNode);
        pang -> {error, {not_found, InitNode}}
    end.

init_successor_list(InitNode) ->
    MyHash = myhash(),
    State = #state {
        myhash = MyHash
    },
    case rpc:call(InitNode, ?MODULE, server, []) of
        {ok, InitServer} -> {ok, State#state{succlist=init_join(InitServer, MyHash)}};
        {badrpc, Reason} -> {stop, Reason}
    end.

init_join(InitServer, MyHash) ->
    gen_server:call(InitServer, {join_op, MyHash}).


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
    case next(NewHash, MySuccList) of
        self ->
            {NewServer, _} = From,
            join_op_1(From, MySuccList, [{NewServer, NewHash} | MySuccList]);
        {nil, self, N} ->
            {NewServer, _} = From,
            join_op_1(From, replace(N, MySuccList, {whereis(?MODULE), MyHash}), [{NewServer, NewHash} | MySuccList]);
        {OtherNode, _} ->
            gen_server:cast(OtherNode, {join_op_cast, NewHash, From}),
            MySuccList
    end.

join_op_1(From, MySuccList, NewSuccList) ->
    gen_server:reply(From, MySuccList),
    io:format("[~p] successor list = ~p~n", [self(), NewSuccList]),
    NewSuccList.


lookup_op() ->
    pass.


%%====================================================================
%% utilities
%%====================================================================
myhash() ->
    crypto:start(),
    crypto:sha_init(),
    crypto:sha(list_to_binary(atom_to_list(node()))).


next(NewHash, [{_, Hash} | _]) when NewHash > Hash ->
    self;
next(_, [nil | _]) ->
    {nil, self, 1};
next(NewHash, SuccList) ->
    next_1(NewHash, SuccList, 1).

next_1(_, [Peer | _], ?LENGTH_SUCCESSOR_LIST) ->
    Peer;
next_1(_, [Peer, nil | _], _) ->
    Peer;
next_1(NewHash, [{_, NewHash} | _], _) ->
    {error, equal_hash};
next_1(NewHash, [{Server, Hash} | _], _) when NewHash > Hash ->
    {Server, Hash};
next_1(NewHash, [{_, Hash} | Tail], Count) when NewHash < Hash ->
    next_1(NewHash, Tail, Count + 1).


replace(_, [], _) ->
    [];
replace(1, [_ | Tail], Y) ->
    [Y | Tail];
replace(N, [X | Tail], Y) ->
    [X | replace(N - 1, Tail, Y)].
