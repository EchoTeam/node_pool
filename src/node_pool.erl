%%% 
%%% Copyright (c) 2008-2014 JackNyfe, Inc. <info@jacknyfe.com>
%%% All rights reserved.
%%%

% vim: ts=4 sts=4 sw=4 expandtab:

-module(node_pool).

-behaviour(gen_server).

-export([
    apply_on_one_node/3,
    call/6,
    erpc/6,
    invoke/3,
    start_link/2,
    status/1,
    lookup_nodes/2
]).

-export([
    code_change/3,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    init/1,
    terminate/2
]).

-ifdef(TEST).
-export([
    try_rpc/5
]).
-endif.

-record(state, {
                down_nodes,
                pool_name,
                ring_pid
               }).

-define(DEFAULT_CALL_TIMEOUT, 15000).

-type call_answer() :: {node(), {'badrpc', 'nodedown' | 'timeout'} | term()}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% start a pool with a predefined list of nodes
start_link(Name, Nodes) when is_list(Nodes) ->
    gen_server:start_link({local, Name}, ?MODULE, [Name, Nodes], []);

% start a pool by getting a list of nodes dynamically by calling the passed module, function and arguments
start_link(Name, {Module, Function, Args}) when is_atom(Module), is_atom(Function), is_list(Args) ->
    Nodes = apply(Module, Function, Args),
    start_link(Name, Nodes).

init([Name, Nodes]) ->
    {ok, RingPid} = dht_ring:start_link([{Node, undefined, 100} || Node <- Nodes]),
    lager:info("Node pool ~p consists of:~n~s", [Name, dht_ring:node_shares(RingPid)]),
    [node_mon:start_link(Node, self()) || Node <- Nodes],
    {ok, #state{down_nodes = sets:from_list(Nodes), pool_name = Name, ring_pid = RingPid}}.

handle_call(state, _From, State) ->
    {reply, State, State};

handle_call(status, _From, State) ->
    Nodes = lists:sort(dht_ring:nodes(State#state.ring_pid)),
    DownNodes = lists:sort(sets:to_list(State#state.down_nodes)),
    Status = {ok, [
            {name, State#state.pool_name},
            {ring_nodes, Nodes},
            {up_nodes, proplists:get_keys(Nodes) -- DownNodes},
            {down_nodes, DownNodes}
        ]},
    {reply, Status, State};

handle_call({lookup, KeyIndex}, _From, State) ->
    Reply = [{Node, Opaque} || {Node, Opaque} <- gen_server:call(State#state.ring_pid, {lookup, KeyIndex}, ?DEFAULT_CALL_TIMEOUT),
                               not sets:is_element(Node, State#state.down_nodes)],
    {reply, Reply, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({node_mon, Node, down}, State) ->
    {noreply, State#state{down_nodes = sets:add_element(Node, State#state.down_nodes)}};

handle_info({node_mon, Node, up}, State) ->
    {noreply, State#state{down_nodes = sets:del_element(Node, State#state.down_nodes)}};

handle_info(_Request, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

-spec lookup_nodes(ServerRef :: atom(), Key :: term()) -> [node()].
lookup_nodes(ServerRef, Key) ->
    State = gen_server:call(ServerRef, state),
    [Node || {Node, _} <- dht_ring:lookup(State#state.ring_pid, Key),
                            not sets:is_element(Node, State#state.down_nodes)].

-spec make_call(IPC :: 'rpc' | 'erpc', ServerRef :: term(), Key :: term(),
                M :: atom(), F :: atom(), A :: [term()], Timeout :: timeout()) -> call_answer().
make_call(IPC, ServerRef, Key, M, F, A, Timeout) ->
    Nodes = lookup_nodes(ServerRef, Key),
    try_rpc(IPC, call, Nodes, [M, F, A, Timeout], undefined).

-spec call(ServerRef :: atom(), Key :: term(), M :: atom(), F :: atom(), A :: [term()], Timeout :: timeout()) -> call_answer().
call(ServerRef, Key, M, F, A, Timeout) ->
    make_call(rpc, ServerRef, Key, M, F, A, Timeout).

-spec erpc(ServerRef :: atom(), Key :: term(), M :: atom(), F :: atom(), A :: [term()], Timeout :: timeout()) -> call_answer().
erpc(ServerRef, Key, M, F, A, Timeout) ->
    make_call(erpc, ServerRef, Key, M, F, A, Timeout).

% @spec invoke(ServerRef, Key, Fun) -> {node(), {badrpc, Reason}} | {node(), Value}.
invoke(ServerRef, Key, Fun) ->
    Nodes = lookup_nodes(ServerRef, Key),
    try_invoke(Nodes, Fun, undefined).

% @spec apply_on_one_node(ServerRef, IntKey, F) -> {Result | ok}.
apply_on_one_node(ServerRef, IntKey, F) ->
    Me = node(),
    case dht_ring:lookup_index(ServerRef, IntKey) of
        [{Me, _} | _] -> F();
        _ -> ok
    end.

% @spec status(ServerRef) -> {ok, [{pool_name, Pool}, {down_nodes, Nodes}]},
status(ServerRef) ->
    gen_server:call(ServerRef, status).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec try_rpc(IPC :: 'rpc' | 'erpc', Type :: 'call', Nodes :: [node()], Args :: [term(), ...],
                DefaultAnswer :: 'undefined' | {node(), {'badrpc', 'nodedown'}}) -> call_answer().
try_rpc(IPC, Type, [Node | Nodes], Args, _DefaultAnswer) ->
    case apply(IPC, Type, [Node | Args]) of
        {badrpc, nodedown} = Error -> try_rpc(IPC, Type, Nodes, Args, {Node, Error});
        {badrpc, _} = Error -> {Node, Error};
        Value -> {Node, Value}
    end;
try_rpc(_IPC, _Type, [], _Args, undefined) -> {node(), {badrpc, nodedown}};
try_rpc(_IPC, _Type, [], _Args, DefaultAnswer) -> DefaultAnswer.

try_invoke([Node | Nodes], Fun, _DefaultAnswer) ->
    case Fun(Node) of
        {badrpc, nodedown} = Error -> try_invoke(Nodes, Fun, {Node, Error});
        {badrpc, _} = Error -> {Node, Error};
        Value -> {Node, Value}
    end;
try_invoke([], _Args, undefined) -> {node(), {badrpc, nodedown}};
try_invoke([], _Args, DefaultAnswer) -> DefaultAnswer.
