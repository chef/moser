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
%% @copyright (C) 2012, Opscode Inc.
%% @doc
%%
%% @end
%% Created :  8 Nov 2012 by Mark Anderson <>
%%-------------------------------------------------------------------
-module(moser_chef_processor).

%% API
-export([process_couch_file/1, cleanup_org_info/1]).

-include("moser.hrl").

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% API
%%%===================================================================

process_couch_file(OrgId) ->
    CData = ets:new(chef_data, [set,public]),
    AData = ets:new(auth_data, [set,public]),

    DbName = lists:flatten(["chef_", OrgId, ".couch"]),

    Org = #org_info{ org_name = "TBD",
                     org_id = OrgId,
                     db_name = DbName,
                     chef_ets = CData,
                     auth_ets = AData,
                     start_time = os:timestamp()},
    IterFn = fun(Key, Body, AccIn) ->
                     process_item(Org, Key, Body),
                     AccIn
             end,
    decouch_reader:open_process_all(DbName, IterFn),
    %% TODO: fix this to get orgname properly (from guid or something)
    case ets:lookup(Org#org_info.chef_ets, orgname) of 
        [] -> Org;
        [{orgname, OrgName}] ->
            Org#org_info{org_name = OrgName}
    end.

cleanup_org_info(#org_info{org_name = Name, org_id = Guid, chef_ets = Chef, auth_ets = Auth, start_time = Start}) ->
    ets:delete(Chef),
    ets:delete(Auth),
    Time = timer:now_diff(os:timestamp(), Start),
    io:format("Database ~s (org ~s) completed in ~f seconds", [Name, Guid, Time/10000000]).


%%--------------------------------------------------------------------
%% @doc
%% @spec
%% @end
%%--------------------------------------------------------------------

%%%===================================================================
%%% Internal functions
%%%===================================================================

process_item(Org, Key, Body) ->
    JClass = ej:get({<<"json_class">>}, Body),
    CRType = ej:get({<<"couchrest-type">>}, Body),
    Type  = case {JClass, CRType} of 
                {undefined, undefined} -> undefined;
                {undefined, T} -> T;
                {T, undefined} -> T;
                _ ->
                    ?debugVal(Key),
                    ?debugVal(Body),
                    ?debugVal({JClass, CRType}),
                    error
            end,
    process_item_by_type(normalize_type_name(Type), Org, Key, Body),
    ok.

%% Process special types
process_item_by_type({_, node}, _Org, _Key, _Body) ->
    %% Node docs are to be ignored
    ok;
%% Process Chef:: types
process_item_by_type({chef, ChefType}, Org, Key, Body) ->
    ets:insert(Org#org_info.chef_ets, {Key, ChefType, Body});
%% Process simple mixlib authorization types
%% These just have the fields
%% id, couchrest-type, name (or some variant), sometimes orgname, requester_id
process_item_by_type({auth_simple, AuthType}, Org, Key, Body) ->
    Name = get_name(AuthType, Body),
    ets:insert(Org#org_info.auth_ets, {{AuthType, Name}, {Key, Body}});
%% More complex mixlib authorization types contain other data, or don't fit the schema
%% So we need to put them into the chef db for further processing...
%%
%% Client: All the info is in the mixlib record; there is no Chef::Client object
process_item_by_type({auth, client=AuthType}, Org, Key, Body) ->
    Name = get_name(AuthType, Body),
    ets:insert(Org#org_info.chef_ets, {{AuthType, Name}, {Key, Body}});
%% Group: actor_and_group_names, groupname, orgname
process_item_by_type({auth, group=AuthType}, Org, Key, Body) ->
    Name = get_name(AuthType, Body),
    %% NOTE: This is a dirty hack for the orgname and we should be ashamed of ourselves.
    %% TODO: RIP THIS OUT when we get things running
    OrgName = ej:get({<<"orgname">>}, Body),
    ets:insert(Org#org_info.chef_ets, {orgname, OrgName}),
    ets:insert(Org#org_info.chef_ets, {{AuthType, Name}, {Key, Body}});

%% Process various unmatched types
process_item_by_type(undefined, _Org, <<"_design/", DesignDoc/binary>>, _Body) ->
    io:format("Design doc ~s~n", [DesignDoc]);
process_item_by_type(undefined, _Org, _Key, _Body) ->
    ?debugVal(undefined),
    ?debugVal(_Key),
    ?debugVal(_Body),
    ok;
process_item_by_type(Type, Org, Key, Body) ->
    ?debugVal(Type),
    ets:insert(Org#org_info.chef_ets, {Key, Body}).

normalize_type_name(<<"Mixlib::Authorization::Models::Client">>) -> {auth, client};
normalize_type_name(<<"Mixlib::Authorization::Models::Container">>) -> {auth_simple, container};
normalize_type_name(<<"Mixlib::Authorization::Models::Cookbook">>) -> {auth_simple, cookbook};
normalize_type_name(<<"Mixlib::Authorization::Models::DataBag">>) -> {auth_simple, databag};
normalize_type_name(<<"Mixlib::Authorization::Models::Environment">>) -> {auth_simple, environment};
normalize_type_name(<<"Mixlib::Authorization::Models::Group">>) -> {auth, group};
normalize_type_name(<<"Mixlib::Authorization::Models::Node">>) -> {auth_simple, node};
normalize_type_name(<<"Mixlib::Authorization::Models::Role">>) -> {auth_simple, role};
normalize_type_name(<<"Mixlib::Authorization::Models::Sandbox">>) -> {auth_simple, sandbox};
normalize_type_name(<<"Chef::Checksum">>) -> {chef, checksum};
normalize_type_name(<<"Chef::CookbookVersion">>) -> {chef, cookbook_version};
normalize_type_name(<<"Chef::DataBag">>) -> {chef, databag};
normalize_type_name(<<"Chef::DataBagItem">>) -> {chef, databag_item};
normalize_type_name(<<"Chef::Environment">>) -> {chef, environment};
normalize_type_name(<<"Chef::Node">>) -> {chef, node};
normalize_type_name(<<"Chef::Role">>) -> {chef, role};
normalize_type_name(<<"Chef::Sandbox">>) -> {chef, sandbox};
normalize_type_name(undefined) -> undefined.


get_name(Type, Body) ->
    ej:get({mixlib_name_key(Type)}, Body).

mixlib_name_key(client) -> <<"clientname">>;
mixlib_name_key(container) -> <<"containername">>;
mixlib_name_key(cookbook) -> <<"display_name">>;
mixlib_name_key(group) -> <<"groupname">>;
mixlib_name_key(sandbox) -> <<"sandbox_id">>;
mixlib_name_key(_) -> <<"name">>.


