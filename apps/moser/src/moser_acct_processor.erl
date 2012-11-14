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
-export([process_account_file/0,
         cleanup_account_info/1
        ]).

-include("moser.hrl").

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% API
%%%===================================================================


process_account_file() ->
    DbName = lists:flatten([moser_chef_processor:get_couch_path(), "opscode_account.couch"]),
    Account = #account_info{
      user_to_authz = ets:new(user_to_authz, [set, public]),
      authz_to_user = ets:new(authz_to_user, [set, public]),
      db = ets:new(account_db, [set, public])
     },

    IterFn = fun(Key, Body, AccIn) ->
                     process_account_item(Account, Key, Body),
                     AccIn
             end,
    decouch_reader:open_process_all(DbName, IterFn),
    Account.

cleanup_account_info(#org_info{org_name = Name, org_id = Guid, chef_ets = Chef, auth_ets = Auth, start_time = Start}) ->
    ets:delete(Chef),
    ets:delete(Auth),
    Time = timer:now_diff(os:timestamp(), Start),
    io:format("Database ~s (org ~s) completed in ~f seconds", [Name, Guid, Time/10000000]).

process_account_item(Account, Key, Body) ->
    Type = moser_chef_processor:extract_type(Key, Body),
    process_item_by_type(normalize_type_name(Type), Account, Key, Body),
    ok.

process_item_by_type(auth_group, 
                     #account_info{db=Db}, Key, Body) ->
    ets:insert(Db, {{auth_group, Key}, Body}),
    ok;
process_item_by_type(auth_join,
                     #account_info{user_to_authz=User2Auth,
                                   authz_to_user=Auth2User},
                     _Key, Body) ->
    UserId = ej:get({<<"user_object_id">>}, Body),
    AuthId = ej:get({<<"auth_object_id">>}, Body),
    ets:insert(User2Auth, {UserId, AuthId}),
    ets:insert(Auth2User, {AuthId, UserId}),
    ok;
process_item_by_type(auth_org, #account_info{db=Db},
                     _Key, Body) ->
    ?debugFmt("~s ~p~n", [auth_org, Body]),
    _Key = Body,
    ok;
%%
%% Catch all
%% 
process_item_by_type(_Type, _Acct, _Key, Body) ->
    ?debugFmt("~s ~p~n", [_Type, Body]),
    ok.

normalize_type_name(<<"Mixlib::Authorization::AuthJoin">>) -> auth_join;
normalize_type_name(<<"Mixlib::Authorization::Models::Group">>) -> auth_group;
normalize_type_name(<<"Mixlib::Authorization::Models::Organization">>) -> auth_org;
normalize_type_name(<<"OrganizationUser">>) -> org_user.
     
