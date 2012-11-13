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

%% This needs to look up the mixlib auth doc, find the user side id and the requestor id, 
%% map the user side id via opscode_account to the auth side id and return a tuple
get_authz_info(_Org, Type, Name, Id) ->
    AuthzId = iolist_to_binary([erlang:atom_to_binary(Type, utf8), "-", Name, "-", Id]),
    RequestorId = AuthzId,
    {AuthzId, RequestorId}.

insert(#org_info{org_name = Name, org_id = Guid, chef_ets = Chef} = Org) ->
    {Time, Totals} = timer:tc(
                  ets, foldl, [fun(Item,Acc) ->
                                       insert_one(Org, Item, Acc)
                               end,
                               dict:new(), Chef] ),
    io:format("Stats: ~p", [dict:to_list(Totals)]),
    io:format("Database ~s (org ~s) insertions took ~f seconds", [Name, Guid, Time/10000000]).

insert_one(Org, {{databag = Type, Id}, Data} = Item, Acc) ->
    Name = ej:get({<<"name">>}, Data),
    {AId, RequestorId} = get_authz_info(Org, Type, Name, Id),
    ?debugVal(Item),
    Obj = #chef_data_bag{
      id = Id, %% TODO do real id conversion
      authz_id = AId,
      org_id = Org#org_info.org_id,
      name = Name },
    ObjWithDate = chef_object:set_created(Obj, RequestorId),
    ?debugVal(ObjWithDate),
    ?debugVal(chef_sql:create_data_bag(ObjWithDate)),
    dict:update_counter(Type, 1, Acc);
insert_one(_Org, {{Type, _Id}, _Data} = _Item, Acc) ->
    dict:update_counter(Type, 1, Acc);
insert_one(_Org, Item, Acc) ->
    ?debugVal(Item),
    Acc.
   

