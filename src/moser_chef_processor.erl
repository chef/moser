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
%% @copyright (C) 2014, Chef Inc.
%% @doc
%%
%% @end
%% Created :  8 Nov 2012 by Mark Anderson <>
%%-------------------------------------------------------------------
-module(moser_chef_processor).

%% API
-export([cleanup_org_info/1,
         extract_type/2,
         process_couch_file/1,
         process_organization/1
        ]).

-include_lib("moser/include/moser.hrl").

-include_lib("eunit/include/eunit.hrl").


%%%===================================================================
%%% API
%%%===================================================================

process_organization(OrgName) ->
    OrgInfo = moser_acct_processor:expand_org_info(#org_info{ org_name = OrgName}),
    process_couch_file(OrgInfo).

process_couch_file(DbFile) when is_list(DbFile) ->
    OrgInfo = moser_acct_processor:expand_org_info(#org_info{ db_name = DbFile }),
    process_couch_file(OrgInfo);
process_couch_file(#org_info{db_name=DbName} = OrgInfo) ->
    case filelib:is_file(DbName) of
        false ->
            lager:error(?LOG_META(OrgInfo), "Can't open file '~s'", [DbName]),
            throw({no_such_file, DbName});
        true ->
            ok
    end,

    CData = ets:new(chef_data, [set,public]),
    AData = ets:new(auth_data, [set,public]),

    Org = OrgInfo#org_info{ chef_ets = CData,
                            auth_ets = AData,
                            start_time = os:timestamp()},
    IterFn = fun(Key, _RevId, Body, AccIn) ->
                     process_couch_item(Org, Key, Body),
                     AccIn
             end,
    decouch_reader:open_process_all(DbName, IterFn),
    {ok, Org}.

cleanup_org_info(#org_info{chef_ets = Chef, auth_ets = Auth}) ->
    ets:delete(Chef),
    ets:delete(Auth).

%% @doc Return the type of a given document from couchdb. We determine the type taking the
%% first type value found among a list of type keys: json_class, couchrest-type, and
%% type. Documents should, in theory, have either json_class xor couchrest-type. The "type"
%% key corresponds to some quick start objects which we ignore. Documents with unknown type
%% return as 'undefined' and are later ignored. We select json_class first since this is the
%% type of checksums, the most prolific object.
extract_type(<<"_design/",_/binary>>, _Body) ->
    design_doc;
extract_type(_Key, Body) ->
    TypeKeyPrefList = [<<"json_class">>, <<"couchrest-type">>, <<"type">>],
    RawType = extract_first_type(TypeKeyPrefList, Body),
    normalize_type_name(RawType).

%%--------------------------------------------------------------------
%% @doc
%% @spec
%% @end
%%--------------------------------------------------------------------

%%%===================================================================
%%% Internal functions
%%%===================================================================


extract_first_type([Key | Rest], Body) ->
    case ej:get({Key}, Body) of
        undefined ->
            extract_first_type(Rest, Body);
        Type ->
            Type
    end;
extract_first_type([], _Body) ->
    undefined.

normalize_type_name(<<"Mixlib::Authorization::Models::Client">>) -> {auth, client};
normalize_type_name(<<"Mixlib::Authorization::Models::Container">>) -> {auth, container};
normalize_type_name(<<"Mixlib::Authorization::Models::Cookbook">>) -> {auth_simple, cookbook};
normalize_type_name(<<"Mixlib::Authorization::Models::DataBag">>) -> {auth_simple, databag};
normalize_type_name(<<"Mixlib::Authorization::Models::Environment">>) -> {auth_simple, environment};
normalize_type_name(<<"Mixlib::Authorization::Models::Group">>) -> {auth, group};
normalize_type_name(<<"Mixlib::Authorization::Models::Node">>) -> {auth_simple, node};
normalize_type_name(<<"Mixlib::Authorization::Models::Role">>) -> {auth_simple, role};
normalize_type_name(<<"Mixlib::Authorization::Models::Sandbox">>) -> {auth_simple, sandbox};
normalize_type_name(<<"Chef::ApiClient">>) -> {chef, apiclient};
normalize_type_name(<<"Chef::Checksum">>) -> {chef, checksum};
normalize_type_name(<<"Chef::Cookbook">>) -> {chef, cookbook};
normalize_type_name(<<"Cookbook">>) -> {chef, cookbook_old};
normalize_type_name(<<"Chef::CookbookVersion">>) -> {chef, cookbook_version};
normalize_type_name(<<"Chef::DataBag">>) -> {chef, databag};
normalize_type_name(<<"Chef::DataBagItem">>) -> {chef, databag_item};
normalize_type_name(<<"Chef::Environment">>) -> {chef, environment};
normalize_type_name(<<"Chef::Node">>) -> {chef, node};
normalize_type_name(<<"Chef::Role">>) -> {chef, role};
normalize_type_name(<<"Chef::Sandbox">>) -> {chef, sandbox};
%% %% acct db types:
normalize_type_name(<<"AssociationRequest">>) -> association_request;
normalize_type_name(<<"Mixlib::Authorization::AuthJoin">>) -> auth_join;
normalize_type_name(<<"Mixlib::Authorization::Models::Organization">>) -> auth_org;
normalize_type_name(<<"Mixlib::Authorization::Models::User">>) -> auth_user;
normalize_type_name(<<"OrganizationUser">>) -> org_user;
%% instance type is part of the defunct quick start feature
normalize_type_name(<<"instance">>) -> undefined;
%% misc
normalize_type_name(design_doc) -> design_doc;
normalize_type_name(undefined) -> undefined.

process_couch_item(Org, Key, Body) ->
    Type = extract_type(Key, Body),
    process_item_by_type(Type, Org, Key, Body),
    ok.

%% Insert item into appropriate ETS table(s) or ignore. Return value should be ignored.
process_item_by_type({_, node}, _Org, _Key, _Body) ->
    %% Node docs are to be ignored
    ok;
process_item_by_type({chef, ChefType = checksum}, Org, _Key, Body) ->
    %% All Chef::Checksum types go into the chef_ets table
    %% keyed by the checksum value.  This prevents duplicate
    %% checksums failing to insert.
    ets:insert(Org#org_info.chef_ets, {{ChefType, ej:get({"checksum"}, Body)}, Body});
process_item_by_type({chef, ChefType}, Org, Key, Body) ->
    %% All Chef::* types go into the chef_ets table.
    ets:insert(Org#org_info.chef_ets, {{ChefType, Key}, Body});
process_item_by_type({auth_simple, AuthType}, Org, Key, Body) ->
    %% Simple mixlib authorization types just have the fields: id, couchrest-type, name (or
    %% some variant), sometimes orgname, and requester_id.
    Name = get_name(AuthType, Body),
    ets:insert(Org#org_info.auth_ets, {{AuthType, Name}, {Key, Body}});
process_item_by_type({auth, client=AuthType}, Org, Key, Body) ->
    %% Client: All the info is in the mixlib record; there is no Chef::Client object
    Name = get_name(AuthType, Body),
    ets:insert(Org#org_info.chef_ets, {{AuthType, Name}, {Key, Body}});
process_item_by_type({auth, container=AuthType}, Org, Key, Body) ->
    %% Container: All the info is in the mixlib record; there is no Chef::Client object
    Name = get_name(AuthType, Body),
    ets:insert(Org#org_info.auth_ets, {{AuthType, Name}, {Key, Body}});
process_item_by_type({auth, group=AuthType}, Org, Key, Body) ->
    %% Group: actor_and_group_names, groupname, orgname
    Name = get_name(AuthType, Body),
    ets:insert(Org#org_info.auth_ets, {{AuthType, Name}, {Key, Body}});
process_item_by_type(design_doc, _, _, _) ->
    ok;
process_item_by_type(undefined, _Org, _Key, _Body) ->
    %% we've handled the types we care to migrate now, so unknown types are otherwise
    %% ignored.
    ok.

get_name(Type, Body) ->
    ej:get({mixlib_name_key(Type)}, Body).

mixlib_name_key(client) -> <<"clientname">>;
mixlib_name_key(container) -> <<"containername">>;
mixlib_name_key(cookbook) -> <<"display_name">>;
mixlib_name_key(group) -> <<"groupname">>;
mixlib_name_key(sandbox) -> <<"sandbox_id">>;
mixlib_name_key(_) -> <<"name">>.
