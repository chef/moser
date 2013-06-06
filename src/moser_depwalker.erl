%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92-*-
%% ex: ts=4 sw=4 et
%%
%% @author Marc Paradise <marc@opscode.com>
%% @copyright 2013, Opscode Inc
%%
%% Tools that will walk through each node in an org, extract its
%% runlist, apply environment constraints for the node, then evaluate
%% via depsolver.
%%
-module(moser_depwalker).

-export([init/0,
         eval_node_deps_for_org_list/1,
         eval_node_deps_for_org/1,
         org_totals/1,
         node_info/2
          ]).

-include_lib("chef_objects/include/chef_types.hrl").

-record(totals, { node = 0, depsolver_timeout = 0,
                  depsolver_unreachable = 0, depsolver_other = 0,
                  depsolver_unsolvable = 0 }).

init() ->
    ets:new(org_node_dep_counts, [public, named_table]).

eval_node_deps_for_org_list(OrgList) ->
    [ eval_node_deps_for_org(Name) || Name <- OrgList ].

eval_node_deps_for_org(OrgName) ->
    Results = moser_db:fetch_nodes(OrgName),
    eval_node_deps(Results, OrgName).

org_totals(OrgName) ->
    fetch_org_node_dep_counts(OrgName).


%% For each provided node,extract its runlist and run it through depsolver.
%% Returns list of results tuples as below and accumulates total node
%% count and depsolver error counts in ets table org_node_dep_counts with a key of
%% OrgName.
%% [ { ok|error, DetailTuple} ]
%% Note that if node or environment fails to load or has an empty/missing run list,
%% this is not considered a dependency error and so is not captured in
%% the results
%%
eval_node_deps([], _) ->
    [];
eval_node_deps(Nodes, OrgName) ->
    % Discardable per-org ets cache for env data
    OrgEnvCache = ets:new(env_cache, []),
    AllVersions = moser_db:fetch_all_cookbook_version_dependencies(OrgName),
    init_org_node_dep_counts(OrgName, length(Nodes)),
    Result = lists:foldl(fun(NodeName, Acc) ->
                    R = eval_deps(node_info(OrgName, NodeName, OrgEnvCache), AllVersions),
                    update_org_node_dep_counts_for_results(OrgName, R),
                    [ {NodeName, R} | Acc ]
            end, [], Nodes),
    log_org_node_dep_stats(OrgName),
    ets:delete(OrgEnvCache),
    Result.

log_org_node_dep_stats(OrgName) ->
    #totals{node = Nodes, depsolver_timeout = Timeouts,
            depsolver_unreachable = Unreachable, depsolver_unsolvable = Unsolvable,
            depsolver_other = Other} = fetch_org_node_dep_counts(OrgName),

    % Simple way to make this output look like json so we can easily parse and play with the results.
    %
    % echo "[" > results.json
    % cat console.log.0 | cut -d# -s -f 2-20 >> results.json
    % echo "]" >> results.json
    %
    EJ = {[{org, OrgName}, {total, Nodes}, {unreachable, Unreachable},
           {timeout, Timeouts}, {unsolvable, Unsolvable}, {other, Other}]},
    lager:info([{org_name, OrgName}], " # ~p,", [binary_to_list(chef_json:encode(EJ))]).

node_info(OrgName, NodeName) ->
    node_info(OrgName, NodeName, undefined).

node_info(OrgName, NodeName, OrgEnvCache) ->
    case moser_db:fetch_node(OrgName, NodeName) of
        {error, Error} ->
            lager:warning([{org_name, OrgName}], "Failed to fetch node ~p with error ~p",
                          [NodeName, Error]),
            {error, {node_fetch_failed, Error}};
        not_found ->
            % Should never happen - our node list originates from the
            % same DB we're querying now
            lager:warning([{org_name, OrgName}], "Node unexpectedly not found... ~p", [NodeName]);
        Node ->
            NodeRaw = chef_db_compression:decompress(Node#chef_node.serialized_object),
            J = chef_json:decode(NodeRaw),
            EnvConstraints = fetch_environment_constraints(OrgName,
                                                           ej:get({<<"chef_environment">>}, J),
                                                           OrgEnvCache),
            RunList = ej:get({<<"automatic">>, <<"recipes">>}, J, []),
            {EnvConstraints, cookbooks_for_runlist(RunList)}
    end.


eval_deps({error, _Any}, _) ->
    {error, _Any};
eval_deps({_, []}, _) ->
    {ok, empty_run_list};
eval_deps({EnvConstraints, Cookbooks}, AllVersions) ->
    NotFound = not_found_cookbooks(AllVersions, Cookbooks),
    case not_found_cookbooks(AllVersions, Cookbooks) of
        ok ->
            Deps = chef_depsolver:solve_dependencies(AllVersions, EnvConstraints, Cookbooks),
            handle_depsolver_results(NotFound, Deps);
        NotFound ->
            handle_depsolver_results(NotFound, ignore)
    end.

fetch_environment_constraints(OrgName, EnvName, undefined) ->
    environment_constraints_from_db(OrgName, EnvName);
fetch_environment_constraints(OrgName, EnvName, OrgCache) ->
    case ets:lookup(OrgCache, EnvName) of
        [] ->
            D = environment_constraints_from_db(OrgName, EnvName),
            ets:insert(OrgCache, {EnvName, D}),
            D;
        [{EnvName, Data}] ->
            Data
    end.



environment_constraints_from_db(OrgName, EnvName) ->
    case moser_db:fetch_environment(OrgName, EnvName) of
        {error, Any} ->
            lager:error([{org_name, OrgName}], "Failed to fetch environment ~p, error: ~p", [EnvName, Any]),
            [];
        not_found ->
            lager:warning([{org_name, OrgName}], "No such environment ~p", [EnvName]),
            [];
        Env ->
            chef_object:depsolver_constraints(Env)
    end.

handle_depsolver_results({not_found, CookbookNames}, _Deps) when is_list(CookbookNames)->
    {error, not_found, CookbookNames};
handle_depsolver_results(_, {error, Error} ) ->
    {error, dep_failed, Error };
handle_depsolver_results(_, {ok, DepList}) ->
    {ok, DepList}.

update_org_node_dep_counts_for_results(OrgName, Results) ->
    Counts = fetch_org_node_dep_counts(OrgName),
    update_org_node_dep_counts(OrgName, dep_counts_for_results(Counts, Results)).

dep_counts_for_results(Counts, {ok, _Results}) ->
    Counts;
dep_counts_for_results(Counts, {error, _ErrorDetail}) -> % non-depsolver error
    Counts;
dep_counts_for_results(Counts, {error, not_found, _Cookbooks}) -> % non-depsolver error
    Counts;
dep_counts_for_results(#totals{depsolver_timeout = Timeout} = Counts, {error, dep_failed, resolution_timeout}) ->
    Counts#totals{depsolver_timeout = (Timeout + 1)};
dep_counts_for_results(#totals{depsolver_unreachable = Unreachable} = Counts, {error, dep_failed, {unreachable_package, _P}}) ->
    Counts#totals{depsolver_unreachable = (Unreachable + 1)};
dep_counts_for_results(#totals{depsolver_unsolvable = Unsolvable} = Counts, {error, dep_failed, Unknown})
        when is_list(Unknown) ->
    Counts#totals{depsolver_unsolvable = (Unsolvable + 1)};
dep_counts_for_results(#totals{depsolver_other = Other} = Counts, {error, dep_failed, _Unknown}) ->
    Counts#totals{depsolver_other = (Other + 1)}.

init_org_node_dep_counts(OrgName, NumNodes) ->
    update_org_node_dep_counts(OrgName, #totals{node = NumNodes}).

update_org_node_dep_counts(OrgName, #totals{} = Counts) ->
    ets:insert(org_node_dep_counts, {OrgName, Counts}),
    Counts.

fetch_org_node_dep_counts(OrgName) ->
    case ets:lookup(org_node_dep_counts, OrgName) of
        [] ->
            #totals{};
        [{OrgName, Totals}] ->
            Totals
    end.

not_found_cookbooks(AllVersions, Cookbooks) ->
    NotFound = [cookbook_name(Cookbook) || Cookbook <- Cookbooks,
                                           cookbook_missing(Cookbook, AllVersions)],
    case NotFound of
        [] -> ok;
        _ -> {not_found, NotFound}
    end.

cookbook_name(Cookbook) when is_binary(Cookbook) ->
    Cookbook;
cookbook_name({Name, _Version}) ->
    Name.

cookbook_missing(CB, AllVersions) when is_binary(CB) ->
    not proplists:is_defined(CB, AllVersions);
cookbook_missing({Name, _Version}, AllVersions) ->
    cookbook_missing(Name, AllVersions).

% lifted in part from chef_wm_depsolver
%
cookbooks_for_runlist(Runlist) ->
    Cookbooks = [ cookbook_for_recipe(chef_cookbook:base_cookbook_name(Item)) || Item <- Runlist ],
    remove_dups(Cookbooks).


cookbook_for_recipe({Recipe, Version}) ->
    {cookbook_for_recipe(Recipe), Version};
cookbook_for_recipe(Recipe) ->
    case re:split(Recipe, <<"::">>) of
        [Cookbook, _Recipe] ->
            Cookbook;
        [Cookbook] ->
            Cookbook
    end.
remove_dups(L) ->
    WithIdx = lists:zip(L, lists:seq(1, length(L))),
    [ Elt || {Elt, _} <- lists:ukeysort(2, lists:ukeysort(1, WithIdx)) ].
