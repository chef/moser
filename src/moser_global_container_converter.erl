%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92 -*-
%% ex: ts=4 sw=4 et
%% @author Tyler Cloke <tyler@getchef.com>
%%
%% Copyright 2014 Chef, Inc. All Rights Reserved.

-module(moser_global_container_converter).

-include("moser.hrl").
-include_lib("oc_chef_authz/include/oc_chef_types.hrl").
-include_lib("chef_objects/include/chef_types.hrl").

-export([
         insert/5
        ]).

insert(Guid, AuthzId, RequesterId, ContainerName, Data) when ContainerName =:= <<"organizations">> orelse ContainerName =:= <<"users">> ->
    Object = chef_object:new_record(oc_chef_container, ?GLOBAL_PLACEHOLDER_ORG_ID, AuthzId, Data),
    ObjWithDate = chef_object:set_created(Object, RequesterId),
    ObjWithOldId = ObjWithDate#oc_chef_container{id = Guid},
    ObjWithFakeOrgId = ObjWithOldId#oc_chef_container{org_id = ?GLOBAL_PLACEHOLDER_ORG_ID},
    moser_chef_converter:try_insert(global_placeholder_org, ObjWithFakeOrgId, Guid, AuthzId);

insert(_Guid, _AuthzId, _RequesterId, ContainerName, _Data) ->
    lager:warning("attempted to insert unexpected type: ~p", [ContainerName]).

