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
         cleanup_account_info/1
        ]).

-include("moser.hrl").

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% API
%%%===================================================================

open_account() ->
    {ok, U2A} = dets:open_file(user_to_authz, []),
    {ok, A2U} = dets:open_file(authz_to_user, []),
    {ok, Db} = dets:open_file(account_db, []),
    #account_info{
                   user_to_authz = U2A,
                   authz_to_user = A2U,
                   db = Db
                 }.

close_account(#account_info{user_to_authz = U2A,
                            authz_to_user = A2U,
                            db = Db}) ->
    dets:close(U2A),
    dets:close(A2U),
    dets:close(Db).

process_account_file() ->
    Account = open_account(),
    DbName = lists:flatten([moser_chef_processor:get_couch_path(), "opscode_account.couch"]),

    IterFn = fun(Key, Body, AccIn) ->
                     process_account_item(Account, Key, Body),
                     AccIn
             end,
    decouch_reader:open_process_all(DbName, IterFn),
    Account.

cleanup_account_info(#account_info{user_to_authz = U2A,
                                   authz_to_user = A2U,
                                   db = Db}) ->
    dets:delete(U2A),
    dets:delete(A2U),
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
                     #account_info{db=Db}, Key, Body) ->
    dets:insert(Db, {{auth_org, Key}, Body}),
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
%%
%% Catch all
%%
process_item_by_type(_Type, _Acct, _Key, Body) ->
    ?debugFmt("~s ~p~n", [_Type, Body]),
    ok.

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
