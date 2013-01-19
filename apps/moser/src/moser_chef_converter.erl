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
-export([insert/1]).

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

shrink_id(X) ->
    binary:replace(X, <<"-">>, <<>>,[global]).


%% This needs to look up the mixlib auth doc, find the user side id and the requestor id,
%% map the user side id via opscode_account to the auth side id and return a tuple
get_authz_info(_Org, _Type, _Name, Id) ->
    AuthzId = shrink_id(Id),
    %% <<AuthzId:30/binary, _Rest/binary>> = iolist_to_binary([erlang:atom_to_binary(Type, utf8), "-", Name, "-", Id]),
    RequestorId = AuthzId,
    {AuthzId, RequestorId}.

insert(#org_info{org_name = Name, org_id = Guid, chef_ets = Chef} = Org) ->
    {Time0, Totals0} = timer:tc(
                        ets, foldl, [fun(Item,Acc) ->
                                             insert_prepass(Org, Item, Acc)
                                     end,
                                     dict:new(), Chef] ),
    io:format("Stats Pass 0: ~p~n", [dict:to_list(Totals0)]),
    {Time1, Totals1} = timer:tc(
                        ets, foldl, [fun(Item,Acc) ->
                                             insert_one(Org, Item, Acc)
                                     end,
                                     Totals0, Chef] ),
    io:format("Stats: ~p~n", [dict:to_list(Totals1)]),
    io:format("Database ~s (org ~s) insertions took ~f seconds~n", [Name, Guid, (Time0+Time1)/10000000]).

%%
%% Checksums need to be inserted before other things
%%
insert_prepass(Org, {{checksum = Type, Name}, Data}, Acc) ->
    %%    ?debugVal({Type, Name}),
    %%    ?debugFmt("~p~n",[Data]),
    OrgId = get_org_id(Org),
    %%    ?debugVal(OrgId),
    Checksum = ej:get({"checksum"}, Data),
    case sqerl:statement(insert_checksum, [OrgId, Checksum], count) of
        {ok, 1} ->
            ok;
        Error ->
            Error
    end,
    dict:update_counter(Type, 1, Acc);
insert_prepass(_Org, {{Type, _Id}, _Data} = _Item, Acc) ->
    RType = list_to_atom("PP_" ++ atom_to_list(Type)),
    dict:update_counter(RType, 1, Acc);
insert_prepass(_Org, Item, Acc) ->
    ?debugVal(Item),
    Acc.


%%
%% client
insert_one(Org, {{client = Type, Name}, {Id, Data}}, Acc) ->
%    ?debugVal({Name, Id}),
%    ?debugFmt("~p~n",[Data]),
    {AId, RequestorId} = get_authz_info(Org, Type, Name, Id),
    {PubKey, PubKeyVersion, IsValidator, IsAdmin} = extract_client_key_info(Data),
    Client = #chef_client{
      id = shrink_id(Id), %% TODO do real id conversion
      authz_id = AId,
      org_id = iolist_to_binary(Org#org_info.org_id),
      name = Name,
      validator = IsValidator,
      admin = IsAdmin,
      public_key = PubKey,
      pubkey_version = PubKeyVersion
     },
    ObjWithDate = chef_object:set_created(Client, RequestorId),
    chef_sql:create_client(ObjWithDate),
    dict:update_counter(Type, 1, Acc);
%%
%% Data bag
insert_one(Org, {{databag = Type, Id}, Data}, Acc) ->
    Name = ej:get({<<"name">>}, Data),
    {AId, RequestorId} = get_authz_info(Org, Type, Name, Id),
    DataBag = #chef_data_bag{
      id = shrink_id(Id), %% TODO do real id conversion
      authz_id = AId,
      org_id = iolist_to_binary(Org#org_info.org_id),
      name = Name
     },
    ObjWithDate = chef_object:set_created(DataBag, RequestorId),
    chef_sql:create_data_bag(ObjWithDate),
    dict:update_counter(Type, 1, Acc);
%%
%% Data bag item
insert_one(Org, {{databag_item = Type, Id}, Data}, Acc) ->
    %%    ?debugVal({Type, Id}),
    %%    ?debugFmt("~p~n",[Data]),
    RawData = ej:get({<<"raw_data">>}, Data),
    DataBagName = ej:get({<<"data_bag">>}, Data),
    ItemName = ej:get({<<"name">>}, Data),
    {_AId, RequestorId} = get_authz_info(Org, Type, DataBagName, Id),
    SerializedObject = jiffy:encode(RawData),
    DataBagItem = #chef_data_bag_item{
      id = shrink_id(Id), %% TODO do real id conversion
      org_id = iolist_to_binary(Org#org_info.org_id),
      data_bag_name = DataBagName,
      item_name = ItemName,
      serialized_object = SerializedObject
     },
    ObjWithDate = chef_object:set_created(DataBagItem, RequestorId),
    chef_sql:create_data_bag_item(ObjWithDate),
    dict:update_counter(Type, 1, Acc);
%%
%% Role
insert_one(Org, {{role = Type, Id}, Data}, Acc) ->
    Name = ej:get({<<"name">>}, Data),
    {AId, RequestorId} = get_authz_info(Org, Type, Name, Id),
    SerializedObject = jiffy:encode(Data),
    Role = #chef_role{
      id = shrink_id(Id), %% TODO do real id conversion
      authz_id = AId,
      org_id = get_org_id(Org),
      name = Name,
      serialized_object = SerializedObject
     },
    ObjWithDate = chef_object:set_created(Role, RequestorId),
    chef_sql:create_role(ObjWithDate),
    dict:update_counter(Type, 1, Acc);
%%
%% Cookbook versions: This is so horridly wrong I'm ashamed, but it probably represents the IOP count properly
%%
insert_one(Org, {{cookbook_version = Type, Id}, Data}, Acc) ->
    ?debugVal({Type, Id}),
    ?debugFmt("~p~n",[list_ej_keys(Data)]),
    Name = ej:get({<<"cookbook_name">>}, Data),
    {AId, RequestorId} = get_authz_info(Org, Type, Name, Id),

    Checksums = extract_all_checksums(?SEGMENTS, Data),
    ?debugFmt("~p~n", [lists:usort(Checksums)]),

    BaseRecord = chef_object:new_record(chef_cookbook_version, get_org_id(Org), AId, Data),
    CookbookVersion = BaseRecord#chef_cookbook_version{id = Id},
    ObjWithDate = chef_object:set_created(CookbookVersion, RequestorId),
    ?debugFmt("~p~n",[ObjWithDate]),
    chef_sql:create_cookbook_version(ObjWithDate),
    exit(foobar),
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
    RType = list_to_atom("TODO_" ++ atom_to_list(Type)),
    dict:update_counter(RType, 1, Acc);
    %Acc;
insert_one(_Org, Item, Acc) ->
    ?debugVal(Item),
    Acc.


get_org_id(#org_info{org_id = OrgId}) ->
    iolist_to_binary(OrgId).


extract_client_key_info(Data) ->
    case ej:get({"certificate"}, Data) of
        undefined ->
            %% figure out something better
            {undefined, undefined, undefined, undefined};
        PubKey ->
            PubKeyVersion =  ?KEY_VERSION,
            {PubKey, PubKeyVersion, false, false}
    end.

list_ej_keys({Ej}) ->
    lists:sort([K || {K,_} <- Ej]).

clear_fields(Fields, Data) ->
    lists:foldl(fun(E,A) ->
                        ej:delete({E},A)
                end,
                Data,
                Fields).

extract_all_checksums(Sections, Data) ->
    lists:flatten([ extract_checksums(ej:get({Section}, Data)) || Section <- Sections ]).

extract_checksums(undefined) ->
    [];
extract_checksums(SegmentData) ->
    [ej:get({"checksum"}, Item) || Item <- SegmentData].
