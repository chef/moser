%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92-*-
%% ex: ts=4 sw=4 et
%%
%% @author Marc Paradise <marc@opscode.com>
%%
%% Contains tools that will scan an org's base dependency graph, looking
%% for packages that can't be resolved
%%
%% Does not attempt to apply environment constraints, which may further
%% increase unresolvable dependencies.
%%

-module(moser_depscanner).
-export([init/0,
         capture_missing_deps/1,
         find_all_missing_deps/1,
         org_missing_deps/1,
         unique_missing_deps_for_org/1]).

-type missing_dep() :: { OwningCookbookName :: binary(),
                         OwningCookbookVersion:: binary(),
                         BadDependencyName :: binary() }.
-type org_missing_deps() :: { OrgName :: binary(), [ missing_dep() ]}.

init() ->
    ets:new(org_missing_deps, [public, named_table]).

%% Note that no records are returned for any org that has no missing
%% deps.
-spec find_all_missing_deps(OrgNames :: [binary()]) -> [ org_missing_deps() ].
find_all_missing_deps(OrgNames) ->
    lists:foldl(fun(OrgName, Acc) ->
                    case org_missing_deps(OrgName) of
                        [] ->
                            Acc;
                        Missing ->
                            [{OrgName, Missing} | Acc]
                    end

                end, [], OrgNames).

%% Determine and reply with the missing deps for a given named org.
-spec org_missing_deps(OrgName :: binary()) -> [missing_dep()].
org_missing_deps(OrgName) ->
    ets:delete(org_missing_deps, OrgName),
    Deps = moser_db:fetch_all_cookbook_version_dependencies(OrgName),
    missing_deps(OrgName, Deps).

%% If an org has been processed return a list of unique missing
%% dependency names for the org. If the org has not been processed or
%% no deps are missing, returns []
-spec unique_missing_deps_for_org(binary()) -> [] | [{binary()}].
unique_missing_deps_for_org(OrgName) ->
    case ets:lookup(org_missing_deps, OrgName) of
        [] ->
            [];
        [{OrgName, Value}] ->
            Value
    end.

%% Helper function that walks through the output of
%% find_all_missing_deps and captures data to several files:
%%
%% /tmp/missing-summary-per-org.txt - summary list of orgs and missing
%% packages for those orgs in the form:
%% OrgName: TotalMissingCount, MissingList
%%
%% /tmp/detail-per-org.txt - human readable listing of each org and each
%% missing dep.
%%
%% /tmp/detail-per-org-raw.txt - formatted erlang term detailing:
%% [
%%   { OrgName, [ {CookbookName, CookbookVersion, DepName } ] }
%% ]
-spec capture_missing_deps(MissingDeps :: [org_missing_deps()]) -> ok.
capture_missing_deps(MissingDeps) ->
    Data = ets:tab2list(org_missing_deps),
    {ok, CountsDev} = file:open("/tmp/missing-summary-per-org.txt", [write]),
    format_per_org_summary(CountsDev, Data),
    file:close(CountsDev),
    {ok, DetailDev} =  file:open("/tmp/detail-per-org.txt", [write]),
    format_results_detail(DetailDev, MissingDeps),
    file:close(DetailDev),
    {ok, DetailRaw} =  file:open("/tmp/detail-per-org-raw.txt", [write]),
    io:fwrite(DetailRaw, "~p", [MissingDeps]),
    file:close(DetailRaw),
    ok.

%%
%% Internal
%%
missing_deps(_OrgName, not_found) ->
    [];
missing_deps(OrgName, World) ->
    lists:foldl(fun({PkgName, VersionInfo}, Acc) ->
                    lists:foldl(fun({Version, Deps}, Acc1) ->
                        lists:foldl(fun({DepName, _DepVersion, _Constraint}, Acc2) ->
                                          case lists:keyfind(DepName, 1, World) of
                                              false ->
                                                store_missing_dep(OrgName, DepName),
                                                [{PkgName, Version, DepName} | Acc2];
                                              _ ->
                                                Acc2
                                          end
                                      end, Acc1, Deps )
                    end, Acc, VersionInfo)
                end, [], World).


store_missing_dep(OrgName, DepName) ->
    MissingList = unique_missing_deps_for_org(OrgName),
    case lists:member(DepName, MissingList) of
        false ->
            ets:insert(org_missing_deps, {OrgName, [DepName|MissingList]}),
            ok;
        _ ->
            ok
    end.

format_per_org_summary(Device, List) ->
    lists:foreach(fun({OrgName, DepList}) ->
                      io:fwrite(Device, "~p: ~p ~p~n", [OrgName, length(DepList), DepList])
                  end, List).

format_results_detail(Device, Results) ->
    lists:foreach(fun({OrgName, MissingDeps}) ->
                    lists:foreach(fun({PkgName, Version, DepName}) ->
                            io:fwrite(Device, "Org ~p has cookbook ~p @ v~p that depends on ~p which is missing.~n",
                                      [binary_to_list(OrgName), binary_to_list(PkgName), binary_to_list(Version), binary_to_list(DepName)])
                    end, MissingDeps)
                 end, Results).

