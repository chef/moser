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
-module(moser_acct_processor).

%% API
-export([open_account/0,
         close_account/1,
         process_account_file/0,
         cleanup_account_info/1,
         user_to_auth/2,
         get_org_guid_by_name/2,
         get_org_by_guid/2,
         get_org/2,
         is_precreated_org/1,
         is_precreated_org/2,
         expand_org_info/1
        ]).

-include("moser.hrl").

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% API
%%%===================================================================

dets_open_file(Name) ->
    Dir = envy:get(moser, moser_dets_dir,
                   "/srv/piab/mounts/moser",
                   string),
    dets:open_file(Name, [{file, lists:flatten([Dir,"/",atom_to_list(Name)])}]).

open_account() ->
    {ok, U2A} = dets_open_file(user_to_authz),
    {ok, A2U} = dets_open_file(authz_to_user),
    {ok, O2G} = dets_open_file(orgname_to_guid),
    {ok, Orgs} = dets_open_file(orgs_by_guid),
    {ok, Db}  = dets_open_file(account_db),
    #account_info{
                   user_to_authz = U2A,
                   authz_to_user = A2U,
                   orgname_to_guid = O2G,
                   orgs_by_guid = Orgs,
                   db = Db
                 }.

close_account(#account_info{user_to_authz = U2A,
                            authz_to_user = A2U,
                            orgname_to_guid = O2G,
                            orgs_by_guid = Orgs,
                            db = Db}) ->
    dets:close(U2A),
    dets:close(A2U),
    dets:close(O2G),
    dets:close(Orgs),
    dets:close(Db).

process_account_file() ->
    Account = open_account(),
    DbName = lists:flatten([moser_converter:get_couch_path(), "opscode_account.couch"]),

    IterFn = fun(Key, Body, AccIn) ->
                     process_account_item(Account, Key, Body),
                     AccIn
             end,
    decouch_reader:open_process_all(DbName, IterFn),
    Account.

cleanup_account_info(#account_info{user_to_authz = U2A,
                                   authz_to_user = A2U,
                                   orgname_to_guid = O2G,
                                   db = Db}) ->
    dets:delete(U2A),
    dets:delete(A2U),
    dets:delete(O2G),
    dets:delete(Db).

process_account_item(Account, Key, Body) ->
    Type = moser_chef_processor:extract_type(Key, Body),
    case Type of
        undefined ->
            ?debugFmt("~s ~s ~p~n", [Type, Key, Body]);
        _ ->
            process_item_by_type(normalize_type_name(Type), Account, Key, Body)
    end,
    ok.

process_item_by_type(auth_group,
                     #account_info{db=Db}, Key, Body) ->
    dets:insert(Db, {{auth_group, Key}, Body}),
    ok;
process_item_by_type(auth_join,
                     #account_info{user_to_authz=User2Auth,
                                   authz_to_user=Auth2User},
                     _Key, Body) ->
    UserId = ej:get({<<"user_object_id">>}, Body),
    AuthId = ej:get({<<"auth_object_id">>}, Body),
    dets:insert(User2Auth, {UserId, AuthId}),
    dets:insert(Auth2User, {AuthId, UserId}),
    ok;
process_item_by_type(auth_org,
                     #account_info{orgname_to_guid=OrgName2Guid,
                                   orgs_by_guid=Orgs},
                     _Key, Body) ->
    OrgName = ej:get({<<"name">>}, Body),
    Guid = ej:get({<<"guid">>}, Body),
    dets:insert(OrgName2Guid, {OrgName, Guid}),
    dets:insert(Orgs, {Guid, Body}),
    ok;
process_item_by_type(auth_user,
                     #account_info{db=Db}, Key, Body) ->
    dets:insert(Db, {{auth_user, Key}, Body}),
    ok;
process_item_by_type(association_request,
                     #account_info{db=Db}, Key, Body) ->
    dets:insert(Db, {{association_request, Key}, Body}),
    ok;
process_item_by_type(org_user,
                     #account_info{db=Db}, Key, Body) ->
    dets:insert(Db, {{org_user, Key}, Body}),
    ok;
process_item_by_type(design_doc,
                     #account_info{db=Db}, Key, Body) ->
    dets:insert(Db, {{org_user, Key}, Body}),
    ok;
%%
%% Catch all
%%
process_item_by_type(_Type, _Acct, _Key, Body) ->
    ?debugFmt("~s ~p~n", [_Type, Body]),
    ok.

%%%
%%% Simplify type names to atoms.
%%%
normalize_type_name(<<"AssociationRequest">>) -> association_request;
normalize_type_name(<<"Mixlib::Authorization::AuthJoin">>) -> auth_join;
normalize_type_name(<<"Mixlib::Authorization::Models::Container">>) -> auth_container;
normalize_type_name(<<"Mixlib::Authorization::Models::Group">>) -> auth_group;
normalize_type_name(<<"Mixlib::Authorization::Models::Organization">>) -> auth_org;
normalize_type_name(<<"Mixlib::Authorization::Models::User">>) -> auth_user;
normalize_type_name(<<"OrganizationUser">>) -> org_user;
normalize_type_name(design_doc) -> design_doc;
normalize_type_name(T) ->
    ?debugFmt("Unknown type ~s~n", [T]),
    unknown_type.

%%%
%%%
%%%
user_to_auth(_, bad_id) ->
    <<"bad_authz_id">>;
user_to_auth(#account_info{user_to_authz = U2A}, Id) ->
    case dets:lookup(U2A, Id) of
        [{Id,V}] -> {ok, V};
        [] -> {fail, authz_id_not_found}
    end.

get_org_guid_by_name(Name,
                     #account_info{orgname_to_guid=OrgName2Guid}) ->
    [{Name, GUID}] = dets:lookup(OrgName2Guid, Name),
    GUID.

get_org_by_guid(GUID,
                #account_info{orgs_by_guid=Orgs}) ->
    [{GUID, OrgData}] = dets:lookup(Orgs, GUID),
    OrgData.

get_org(GUID_or_Name,
        #account_info{orgname_to_guid=OrgName2Guid} = AInfo) ->
    GUID = case dets:lookup(OrgName2Guid, GUID_or_Name) of
               [{GUID_or_Name, G}] -> G;
               _ -> GUID_or_Name
           end,
    get_org_by_guid(GUID, AInfo).

is_precreated_org(OrgData) when is_tuple(OrgData) ->
    case ej:get({"full_name"}, OrgData) of
        <<"Pre-created">> ->
            true;
        _ ->
            false
    end.

is_precreated_org(GUID_or_Name, AInfo) ->
    is_precreated_org(get_org(GUID_or_Name, AInfo)).


expand_org_info(#org_info{account_info = undefined} = OrgInfo) ->
    AcctInfo = moser_acct_processor:open_account(),
    expand_org_info(OrgInfo#org_info{account_info = AcctInfo});
expand_org_info(#org_info{org_name = OrgName, org_id = undefined, db_name = undefined,
                          account_info = #account_info{} = Acct} = OrgInfo) ->
    OrgDesc = get_org(OrgName, Acct),
    OrgId = ej:get({"guid"}, OrgDesc),
    DbName = moser_utils:get_dbname_from_orgid(OrgId),
    OrgInfo#org_info{org_id = OrgId, db_name = DbName, is_precreated = is_precreated_org(OrgDesc)};
expand_org_info(#org_info{org_name = undefined, org_id = OrgId, db_name = undefined,
                          account_info = #account_info{} = Acct} = OrgInfo) ->
    OrgDesc = get_org_by_guid(OrgId, Acct),
    OrgName = ej:get({"name"}, OrgDesc),
    DbName = moser_utils:get_dbname_from_orgid(OrgId),
    OrgInfo#org_info{org_name = OrgName, db_name = DbName, is_precreated = is_precreated_org(OrgDesc)};
expand_org_info(#org_info{org_name = undefined, org_id = undefined, db_name = DbName,
                          account_info = #account_info{} = Acct} = OrgInfo) ->
    OrgId = moser_utils:get_orgid_from_dbname(DbName),
    OrgDesc = get_org_by_guid(OrgId, Acct),
    OrgName = ej:get({"name"}, OrgDesc),
    OrgInfo#org_info{org_name = OrgName, org_id = OrgId, is_precreated = is_precreated_org(OrgDesc)}.
