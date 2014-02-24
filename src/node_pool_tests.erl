%%% 
%%% Copyright (c) 2008-2014 JackNyfe, Inc. <info@jacknyfe.com>
%%% All rights reserved.
%%%
%%% vim: ts=4 sts=4 sw=4 expandtab:

-module(node_pool_tests).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-export([
    test_callback/1
]).

test_callback(Fun) ->
    Fun().

try_rpc_test_() ->
    Fun = fun() ->
        ok
    end,
    [{Title, fun() ->
        ?assertEqual(Expectation, node_pool:try_rpc(erpc, call, Nodes, [?MODULE, test_callback, [Fun]], undefined))
    end} || {Title, {Expectation, Nodes}} <- [
        {"no alive nodes",
            {{undefined, {badrpc, nodedown}}, [undefined]}},
        {"first node alive",
            {{node(), ok}, [node(), undefined]}},
        {"second node alive",
            {{node(), ok}, [undefined, node()]}},
        {"empty nodes list",
            {{node(), {badrpc, nodedown}}, []}}
    ]].

try_rpc_error_test() ->
    Tab = ets:new(data, [private, set]),
    try
        ets:insert_new(Tab, {counter, 0}),
        Fun = fun() ->
            [{counter, Counter}] = ets:lookup(Tab, counter),
            ets:update_counter(Tab, counter, 1),
            case Counter of
                1 ->
                    {badrpc, timeout};
                _ ->
                    ok
            end
        end,
        Request = fun() ->
            node_pool:try_rpc(erpc, call, [node(), node()], [?MODULE, test_callback, [Fun]], undefined)
        end,
        % <counter> = 0
        ?assertEqual({node(), ok}, Request()),
        % <counter> = 1. Do not send request to second node from the nodes list
        ?assertEqual({node(), {badrpc, timeout}}, Request()),
        % <counter> = 2. Therefore here is only 3rd request
        ?assertEqual({node(), ok}, Request())
    after
        ets:delete(Tab)
    end.
    
-endif.
