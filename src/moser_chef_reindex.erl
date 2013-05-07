%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92 -*-
%% ex: ts=4 sw=4 et
%%
%%
%% Licensed under the Apache License, Version 2.0 (the "License"); you may not
%% use this file except in compliance with the License. You may obtain a copy of
%% the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
%% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
%% License for the specific language governing permissions and limitations under
%% the License.
%%-------------------------------------------------------------------
%% @author Mark Anderson <mark@opscode.com>
%% @copyright (C) 2013, Opscode Inc.
%% @doc
%% Reindexing an organization from the chef database. This is used in the event
%% we must migrate back to the data in the chef_*** instead of sql.
%% This will purge SOLR of the migrated types, and iterate through the chef_* database
%% and update the index.
%% @end
%% Created :  6 May 2013 by Mark Anderson <mark@opscode.com>
%%-------------------------------------------------------------------
-module(moser_chef_reindex).

%% API
-export([purge_migrated/1,
         reindex_org/1]).

-include("moser.hrl").

%%
%% This is everything *except* nodes; we don't want to delete those.
-define(MIGRATED_TYPES, [client, data_bag_item, environment, role]).
-define(EMPTY_EJSON_HASH, {[]}).

reindex_org(OrgName) when is_list(OrgName) ->
    reindex_org(iolist_to_binary(OrgName));
reindex_org(OrgName) when is_binary(OrgName) ->
    AcctInfo = moser_acct_processor:open_account(),
    case moser_acct_processor:expand_org_info( #org_info{ org_name = OrgName, account_info = AcctInfo } ) of
        not_found ->
            Props = [{error_type, org_not_found}],
            lager:error(Props, "Failed to find org for reindex: ~p", [OrgName]),
            {error, not_found};
        OrgInfo ->
            moser_utils:load_process_org(OrgInfo,
                                         fun reindex_org_internal/1,
                                         fun moser_chef_processor:cleanup_org_info/1,
                                         "REINDEX")
    end.

reindex_org_internal(#org_info{org_name = OrgName,
                          org_id = OrgId,
                          chef_ets = Chef} = Org) ->
    purge_migrated(OrgId),

    Totals = dict:new(),
    Inserter = fun(Item, Acc) ->
                       try
                           reindex(Org, Item, Acc)
                       catch
                           throw:{EType, EDetail} ->
                               RealType = moser_utils:type_for_object(Item),
                               Props = [{error_type, {RealType, EType}} | ?LOG_META(Org)],
                               lager:error(Props, "REINDEX_FAILED ~p ~p ~p",
                                           [EDetail, Item, erlang:get_stacktrace()]),
                               Acc;
                           Error:Why ->
                               RealType = moser_utils:type_for_object(Item),
                               lager:error(?LOG_META(Org), "~s REINDEX_FAILED ~p ~p ~p ~p",
                                           [RealType,
                                            Error, Why, Item, erlang:get_stacktrace()]),
                               Acc
                       end
               end,
    {Time, Totals1} = timer:tc(fun() -> ets:foldl(Inserter, Totals, Chef) end),
    lager:info(?LOG_META(Org), "~p (~p) Reindex Stats: ~p~n",
               [OrgName, OrgId, lists:sort(dict:to_list(Totals1))]),
    lager:info(?LOG_META(Org), "~p (~p) index insertions took ~.3f seconds~n",
               [OrgName, OrgId, moser_utils:us_to_secs(Time)]),
    {Time, Totals1}.

simple_reindex(#org_info{org_id = OrgId}, Type, Id, Ejson, Acc) ->
    chef_index_queue:set(Type, Id, chef_otto:dbname(OrgId), Ejson),
    dict:update_counter(Type, 1, Acc).

reindex(Org, {{client = Type, _Name}, {OldId, Data}}, Acc) ->
    simple_reindex(Org, Type, OldId, Data, Acc);
reindex(Org, {{databag_item = Type, OldId}, Data}, Acc) ->
    NData = databag_item_for_index(Data),
    simple_reindex(Org, Type, OldId, NData, Acc);
reindex(Org, {{environment = Type, OldId}, Data}, Acc) ->
    simple_reindex(Org, Type, OldId, Data, Acc);
reindex(Org, {{role = Type, OldId}, Data}, Acc) ->
    NData = role_for_index(Data),
    simple_reindex(Org, Type, OldId, NData, Acc);
reindex(_Org, {{_Type, _OldId}, _}, Acc) ->
    Acc.

purge_migrated(OrgId) ->
    [chef_solr:delete_search_db_by_type(OrgId, Type) || Type <- ?MIGRATED_TYPES].

copy_key(Key, Src, Dst) ->
    V = ej:get(Key, Src),
    ej:set(Key, Dst, V).

copy_keys(Keys, Src, Dst) ->
    lists:foldl(fun(E, A) -> copy_key(E, Src, A) end, Dst, Keys).

%% These routines are derived from chef_object:ejson_for_indexing

%% Databags have a two layer structure with the actual data in a 'raw_data' field.
databag_item_for_index(Data) ->
    Raw = ej:get({"raw_data"}, Data),
    copy_keys([{"name"}, {"data_bag"}, {"chef_type"}], Data, Raw).

role_for_index(Data) ->
    EnvironmentRunLists0 = ej:get({<<"env_run_lists">>}, Data, ?EMPTY_EJSON_HASH),
    EnvironmentRunLists = ej:delete({<<"_default">>}, EnvironmentRunLists0),
    ej:set({<<"env_run_lists">>}, Data, EnvironmentRunLists).
