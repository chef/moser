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
%% @copyright (C) 2014, Chef Inc.
%% @doc
%%
%% @end
%% Created :  8 Nov 2012 by Mark Anderson <>
%%-------------------------------------------------------------------
-module(moser_group_converter).

%% API
-export([insert/1,
         cleanup_organization/1,
         cleanup_orgid/1,
         cleanup_all/0]).
-include_lib("stdlib/include/qlc.hrl").
-include("moser.hrl").
-include_lib("ej/include/ej.hrl").
-include_lib("chef_objects/include/chef_types.hrl").
-include_lib("oc_chef_authz/include/oc_chef_types.hrl").

-include_lib("eunit/include/eunit.hrl").

%% Order is important because of foreign key constraints on
%% cookbook_version_checksums, checksums, cookbook_versions, and cookbooks
-define(SQL_TABLES, ["containers",
                     "groups"]).


insert(#org_info{} = Org) ->
    try
        {Time0, _Totals0} = insert_objects(Org, dict:new()),
        TotalTime = Time0,
        lager:info(?LOG_META(Org), "inserts complete ~.3f seconds",
                   [moser_utils:us_to_secs(TotalTime)]),
        {ok, TotalTime}
    catch
        error:E ->
            lager:error(?LOG_META(Org), "~p~n~p", [E, erlang:get_stacktrace()]),
            {error, E}
    end.

insert_objects(#org_info{} = Org, Totals) ->
    insert_objects(Org, Totals, fun insert_one/3, "object").

insert_objects(#org_info{org_name = OrgName,
                         org_id = OrgId,
                         chef_ets = _Chef,
                         auth_ets = Auth} = Org,
               Totals, InsertFun, Type) ->

    Inserter = fun(Item, Acc) ->
                       try
                           InsertFun(Org, Item, Acc)
                       catch
                           throw:{EType, EDetail} ->
                               moser_chef_converter:maybe_log_throw(Org, Item, EType, EDetail),
                               Acc;
                           throw:#ej_invalid{msg = Msg, type = SpecType, found = Found, key = Key} ->
                               RealType = moser_utils:type_for_object(Item),
                               Props = [{error_type, {RealType, SpecType, Key, Found}}| ?LOG_META(Org)],
                               lager:error(Props, "FAILED ~p ~p",
                                           [Msg, Item]),
                               Acc;
                           Error:Why ->
                               moser_chef_converter:maybe_log_error(Org, Item, Error, Why),
                               Acc
                       end
               end,
    {Time, Totals1} = timer:tc(fun() -> ets:foldl(Inserter, Totals, Auth) end),
    lager:info(?LOG_META(Org), "~p (~p) Insert ~s Stats: ~p~n",
               [OrgName, OrgId, Type, lists:sort(dict:to_list(Totals1))]),
    lager:info(?LOG_META(Org), "~p (~p) ~s insertions took ~.3f seconds~n",
               [OrgName, OrgId, Type, moser_utils:us_to_secs(Time)]),
    {Time, Totals1}.

get_authz_info(Org, Type, Name, Id, Data) ->
    RequesterId = ej:get({"requester_id"}, Data),
    case moser_chef_converter:user_to_auth(Org, Id) of
        {ok, AuthId} ->
            {AuthId,  RequesterId};
        {fail, FailType} ->
            Props = [{error_type, FailType} | ?LOG_META(Org)],
            lager:warning(Props, "SKIPPING ~s ~s (~s) missing authz data",
                          [Type, Name, Id]),
            not_found
    end.

insert_one(Org, {{Type, Name}, {Id, Data}}, Acc) when Type =:= container orelse Type =:= group ->
    case get_authz_info(Org, Type, Name, Id, Data) of
        {AuthzId, RequesterId} ->
            OrgId = moser_utils:get_org_id(Org),
            CreatorFun =
                case Type of
                    container ->
                        fun() -> chef_object:new_record(oc_chef_container, OrgId, AuthzId, Data) end;
                    group ->
                        fun() -> chef_object:new_record(oc_chef_group, OrgId, AuthzId, Data) end
                end,
            Object = CreatorFun(),
            ObjWithDate = chef_object:set_created(Object, RequesterId),
            ObjWithOldId = set_id(ObjWithDate, Id),
            moser_chef_converter:try_insert(Org, ObjWithOldId, Id, AuthzId),
            dict:update_counter(Type, 1, Acc);
        not_found ->
            Acc
    end;
insert_one(_Org, {orgname, _}, Acc) ->
    %% orgs are ignored for now
    Acc;
insert_one(_Org, Item, Acc) ->
    %% ignore, but log other unhandled items
    lager:warning("unexpected item in insert_one ~p", [Item]),
    Acc.

%% Set the object ID for a chef-object-style record. This is needed because we will not be normalizing
%% the IDs at this time due to complexity related to search and SOLR indexing.
set_id(#oc_chef_container{} = Object, Id) ->
    Object#oc_chef_container{id = Id};
set_id(#oc_chef_group{} = Object, Id) ->
    Object#oc_chef_group{id = Id}.

%%
%% Utility routines
%%

cleanup_organization(OrgName) ->
    cleanup_orgid(moser_utils:orgname_to_guid(OrgName)).

cleanup_orgid(OrgId) ->
    [ moser_chef_converter:delete_table_for_org(Table, OrgId) || Table <- ?SQL_TABLES ].

cleanup_all() ->
    [ moser_chef_converter:sqerl_delete_helper(Q, all) || Q <- ?SQL_TABLES ].
