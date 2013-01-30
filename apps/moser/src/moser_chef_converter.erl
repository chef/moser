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
-module(moser_chef_converter).

%% API
-export([insert/1,
         cleanup_org/1,
         cleanup_all/0]).

-include("moser.hrl").

-include_lib("chef_objects/include/chef_types.hrl").

-include_lib("eunit/include/eunit.hrl").

-define(SEGMENTS,  [<<"attributes">>,
                    <<"definitions">>,
                    <<"files">>,
                    <<"libraries">>,
                    <<"providers">>,
                    <<"recipes">>,
                    <<"resources">>,
                    <<"root_files">>,
                    <<"templates">> ]).



insert(#org_info{org_name = Name, org_id = Guid} = Org) ->
    {Time0, Totals0} = insert_checksums(Org, dict:new()),
    {Time1, Totals1} = insert_databags(Org, Totals0),
    {Time2, _} = insert_objects(Org, Totals1),
    TotalTime = Time0 + Time1 + Time2,
    io:format("Total Database ~s (org ~s) insertions took ~f seconds~n", [Name, Guid, (TotalTime)/10000000]),
    TotalTime.

%%
%% Checksums need to be inserted before other things
%%
insert_checksums(#org_info{org_name = Name, org_id = Guid, chef_ets = Chef} = Org, Totals) ->
    {Time, Totals1} = timer:tc(
                       ets, foldl, [fun(Item,Acc) ->
                                            insert_checksums(Org, Item, Acc)
                                    end,
                                    Totals, Chef] ),
    io:format("Insert Checksums Stats: ~p~n", [lists:sort(dict:to_list(Totals1))]),
    io:format("Database ~s (org ~s) insertions took ~f seconds~n", [Name, Guid, Time/10000000]),
    {Time, Totals1}.


insert_checksums(Org, {{checksum = Type, _Name}, Data}, Acc) ->
    %%    ?debugVal({Type, Name}),
    %%    ?debugFmt("~p~n",[Data]),
    OrgId = moser_utils:get_org_id(Org),
    %%    ?debugVal(OrgId),
    Checksum = ej:get({"checksum"}, Data),
    case sqerl:statement(insert_checksum, [OrgId, Checksum], count) of
        {ok, 1} ->
            ok;
        Error -> %% TODO check errors better here
            Error
    end,
    dict:update_counter(Type, 1, Acc);
insert_checksums(_Org, {{_Type, _Id}, _Data} = _Item, Acc) ->
    RType = list_to_atom("SKIP_CK_" ++ atom_to_list(_Type)),
    dict:update_counter(RType, 1, Acc);
%    Acc;
insert_checksums(_Org, {orgname,_}, Acc) ->
    Acc;
insert_checksums(_Org, Item, Acc) ->
    ?debugVal(Item),
    Acc.

%%
%% Databags need to be inserted before other things
%%
insert_databags(#org_info{org_name = Name, org_id = Guid, chef_ets = Chef} = Org, Totals) ->
    {Time, Totals1} = timer:tc(
                       ets, foldl, [fun(Item,Acc) ->
                                            insert_databag(Org, Item, Acc)
                                    end,
                                    Totals, Chef] ),
    io:format("Insert Databags Stats: ~p~n", [lists:sort(dict:to_list(Totals1))]),
    io:format("Database ~s (org ~s) insertions took ~f seconds~n", [Name, Guid, Time/10000000]),
    {Time, Totals1}.

insert_databag(Org, {{databag, Id}, Data}, Acc) ->
    InsertedType = process_databag(Org, Id, Data),
    dict:update_counter(InsertedType, 1, Acc);
insert_databag(_Org, {{_Type, _Id}, _Data} = _Item, Acc) ->
    RType = list_to_atom("SKIP_DB_" ++ atom_to_list(_Type)),
    dict:update_counter(RType, 1, Acc);
%    Acc;
insert_databag(_Org, {orgname,_}, Acc) ->
    Acc;
insert_databag(_Org, Item, Acc) ->
    ?debugVal(Item),
    Acc.


%%
%% Insert remaining objects
%%
insert_objects(#org_info{org_name = Name, org_id = Guid, chef_ets = Chef} = Org, Totals) ->
    {Time, Totals1} = timer:tc(
                       ets, foldl, [fun(Item,Acc) ->
                                            insert_one(Org, Item, Acc)
                                    end,
                                    Totals, Chef] ),
    io:format("Insert Objects Stats: ~p~n", [lists:sort(dict:to_list(Totals1))]),
    io:format("Database ~s (org ~s) insertions took ~f seconds~n", [Name, Guid, Time/10000000]),
    {Time, Totals1}.


%%
%% client
insert_one(Org, {{client = Type, Name}, {Id, Data}}, Acc) ->
%%    ?debugVal({Name, Id}),
%%    ?debugFmt("~p~n",[Data]),
    AId = user_to_auth(Org, Id), %% Clients are special; they don't contain an authz id field, but their Id is the user side id
    RequesterId = AId, %% TODO: Check that this is right
%%    ?debugVal(AId),
    {PubKey, PubKeyVersion, IsValidator, IsAdmin} = extract_client_key_info(Data),
    Client = #chef_client{
      id = moser_utils:fix_chef_id(Id),
      authz_id = AId,
      org_id = iolist_to_binary(Org#org_info.org_id),
      name = Name,
      validator = IsValidator,
      admin = IsAdmin,
      public_key = PubKey,
      pubkey_version = PubKeyVersion
     },
    ObjWithDate = chef_object:set_created(Client, RequesterId),
    {ok, 1} = chef_sql:create_client(ObjWithDate),
    dict:update_counter(Type, 1, Acc);
%%
%% Data bag item
insert_one(Org, {{databag_item = Type, Id}, Data}, Acc) ->
    %%    ?debugVal({Type, Id}),
    %%    ?debugFmt("~p~n",[Data]),
    RawData = ej:get({<<"raw_data">>}, Data),
    DataBagName = ej:get({<<"data_bag">>}, Data),
    ItemName = ej:get({<<"name">>}, Data),
    {_AId, RequesterId} = get_authz_info(Org, Type, DataBagName, Id),
    SerializedObject = jiffy:encode(RawData),
    DataBagItem = #chef_data_bag_item{
      id = moser_utils:fix_chef_id(Id),
      org_id = iolist_to_binary(Org#org_info.org_id),
      data_bag_name = DataBagName,
      item_name = ItemName,
      serialized_object = SerializedObject
     },
    ObjWithDate = chef_object:set_created(DataBagItem, RequesterId),
    case chef_sql:create_data_bag_item(ObjWithDate) of
        {ok, 1} ->
            ok;
        {foreign_key, Msg} ->
            ?debugFmt("Error inserting ~s, ~s ~n~p~n", [Type, Msg, ObjWithDate]);
        {conflict, Msg} ->
            ?debugFmt("Error inserting ~s, ~s ~n~p~n", [Type, Msg, ObjWithDate])
    end,
    dict:update_counter(Type, 1, Acc);
%%
%% Role
insert_one(Org, {{role = Type, Id}, Data}, Acc) ->
    Name = ej:get({<<"name">>}, Data),
    {AId, RequesterId} = get_authz_info(Org, Type, Name, Id),
    SerializedObject = jiffy:encode(Data),
    Role = #chef_role{
      id = moser_utils:fix_chef_id(Id),
      authz_id = AId,
      org_id = moser_utils:get_org_id(Org),
      name = Name,
      serialized_object = SerializedObject
     },
    ObjWithDate = chef_object:set_created(Role, RequesterId),
    {ok, 1} = chef_sql:create_role(ObjWithDate),
    dict:update_counter(Type, 1, Acc);
%%
%% Environments
insert_one(Org, {{environment = Type, Id}, Data}, Acc) ->
    Name = ej:get({<<"name">>}, Data),
    {AId, RequesterId} = get_authz_info(Org, Type, Name, Id),
    SerializedObject = jiffy:encode(Data),
    Role = #chef_environment{
      id = moser_utils:fix_chef_id(Id),
      authz_id = AId,
      org_id = moser_utils:get_org_id(Org),
      name = Name,
      serialized_object = SerializedObject
     },
    ObjWithDate = chef_object:set_created(Role, RequesterId),
    {ok, 1} = chef_sql:create_environment(ObjWithDate),
    dict:update_counter(Type, 1, Acc);
%%
%% Cookbook versions: This is so horridly wrong I'm ashamed, but it probably represents the IOP count properly
%%
insert_one(Org, {{cookbook_version = Type, Id}, Data}, Acc) ->
%    ?debugVal({Type, Id}),
%    ?debugFmt("~p~n",[moser_utils:list_ej_keys(Data)]),
    Name = ej:get({<<"cookbook_name">>}, Data),
    {AId, RequesterId} = get_authz_info(Org, Type, Name, Id),

    _Checksums = extract_all_checksums(?SEGMENTS, Data),
%    ?debugFmt("~p~n", [lists:usort(Checksums)]),

    BaseRecord = chef_object:new_record(chef_cookbook_version, moser_utils:get_org_id(Org), AId, Data),
    CookbookVersion = BaseRecord#chef_cookbook_version{id = moser_utils:fix_chef_id(Id)},
    ObjWithDate = chef_object:set_created(CookbookVersion, RequesterId),
%    ?debugFmt("~p~n",[ObjWithDate]),
    {ok, 1} = chef_sql:create_cookbook_version(ObjWithDate),
    dict:update_counter(Type, 1, Acc);

%%%
%%% Handled in pass zero
%%%
insert_one(_Org, {{checksum, _Id}, _}, Acc) ->
    Acc;
%%
%% Unhandled objects
%%
insert_one(_Org, {{Type, _Id}, _Data} = _Item, Acc) ->
    RType = list_to_atom("SKIP_P2_" ++ atom_to_list(Type)),
    dict:update_counter(RType, 1, Acc);
%    Acc;
insert_one(_Org, Item, Acc) ->
    ?debugVal(Item),
    Acc.


%%
%% Data bag insertion
%%
process_databag(Org, Id, Data) ->
    Name = ej:get({<<"name">>}, Data),
    {AId, RequesterId} = get_authz_info(Org, databag, Name, Id),
    DataBag = #chef_data_bag{
      id = moser_utils:fix_chef_id(Id),
      authz_id = AId,
      org_id = iolist_to_binary(Org#org_info.org_id),
      name = Name
     },
    ObjWithDate = chef_object:set_created(DataBag, RequesterId),
    {ok, 1} = chef_sql:create_data_bag(ObjWithDate),
    databag.


%%
%% Utility routines
%%
extract_client_key_info(Data) ->
    case ej:get({"certificate"}, Data) of
        undefined ->
            %% figure out something better
            {undefined, undefined, undefined, undefined};
        PubKey ->
            PubKeyVersion =  ?KEY_VERSION,
            {PubKey, PubKeyVersion, false, false}
    end.


extract_all_checksums(Sections, Data) ->
    lists:flatten([ extract_checksums(ej:get({Section}, Data)) || Section <- Sections ]).

extract_checksums(undefined) ->
    [];
extract_checksums(SegmentData) ->
    [ej:get({"checksum"}, Item) || Item <- SegmentData].


%% This needs to look up the mixlib auth doc, find the user side id and the requester id,
%% map the user side id via opscode_account to the auth side id and return a tuple
get_authz_info(Org, Type, Name, Id) ->
    {UserId, RequesterId} = get_user_side_auth_id(Org,Type,Name,Id),
    AuthId = user_to_auth(Org, UserId),
    {AuthId, RequesterId}.

%%
%% This needs to look up the mixlib auth doc, find the user side id and the requester id,
%% map the user side id via opscode_account to the auth side id and return a tuple
get_user_side_auth_id(#org_info{auth_ets=Auth}, cookbook_version, Name, _Id) ->
    [{_, {UserId, Data}}] = ets:lookup(Auth, {cookbook, Name}),
    Requester = ej:get({"requester_id"}, Data),
    {UserId, Requester};
get_user_side_auth_id(#org_info{auth_ets=Auth}, databag_item, Name, _Id) ->
    get_user_side_auth_id_generic(Auth, databag, Name);
get_user_side_auth_id(#org_info{auth_ets=Auth}, databag, Name, _Id) ->
    get_user_side_auth_id_generic(Auth, databag, Name);
get_user_side_auth_id(#org_info{auth_ets=Auth}, role, Name, _Id) ->
    get_user_side_auth_id_generic(Auth, role, Name);
get_user_side_auth_id(#org_info{auth_ets=Auth}, environment = Type, Name, _Id) ->
    get_user_side_auth_id_generic(Auth, Type, Name);
get_user_side_auth_id(_Org, Type, Name, Id) ->
    ?debugFmt("Can't process for type ~s, ~s ~s", [Type, Name, Id]),
    {bad_id, <<"BadId">>}.

get_user_side_auth_id_generic(Auth, Type, Name) ->
    case ets:lookup(Auth, {Type, Name}) of
        [{_, {UserId, Data}}] -> 
            Requester = ej:get({"requester_id"}, Data),
            {UserId, Requester};
        [] ->
            %% TODO: Fix this to hard fail on error,
            ?debugFmt("Can't find auth info for ~s, ~s~n", [Type, Name]),
            {<<"DEADBEEF">>, <<"DEADD0G">>}
    end.

user_to_auth(#org_info{account_info=Acct}, UserId) ->
    moser_acct_processor:user_to_auth(Acct, UserId).

cleanup_org(#org_info{org_id = _OrgId}) ->
    foo.

cleanup_all() ->
    Query =
        <<"delete from cookbook_version_checksums;" %% cookbook_version_checksums, checksums,
          "delete from checksums;"            %% cookbook_versions, and cookbooks
          "delete from cookbook_versions;"
          "delete from cookbooks;"
          "delete from environments;"
          "delete from roles;"
          "delete from clients;"
          "delete from data_bags;"
          "delete from data_bag_items;">>,
    case sqerl:execute(Query) of
        {ok, X} ->
            ?debugVal(X);
        {error, Error} ->
            ?debugFmt("Cleanup error ~p", [Error])
    end.
