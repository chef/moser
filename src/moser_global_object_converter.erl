%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92 -*-
%% ex: ts=4 sw=4 et
%% @author Tyler Cloke <tyler@getchef.com>
%%
%% Copyright 2014 Chef, Inc. All Rights Reserved.

-module(moser_global_object_converter).

-include("moser.hrl").
-include_lib("oc_erchef/include/chef_types.hrl").
-include_lib("oc_erchef/include/oc_chef_types.hrl").

-export([
         insert_container/4,
         insert_group/4
        ]).

insert_container(Guid, AuthzId, RequesterId, Data) ->
    Object = chef_object:new_record(oc_chef_container, ?GLOBAL_PLACEHOLDER_ORG_ID, AuthzId, Data),
    ObjWithDate = chef_object:set_created(Object, RequesterId),
    ObjWithOldId = ObjWithDate#oc_chef_container{id = Guid},
    ObjWithFakeOrgId = ObjWithOldId#oc_chef_container{org_id = ?GLOBAL_PLACEHOLDER_ORG_ID},
    moser_chef_converter:try_insert(global_placeholder_org, ObjWithFakeOrgId, Guid, AuthzId).

insert_group(Guid, AuthzId, RequesterId, Data) ->
    Object = chef_object:new_record(oc_chef_group, ?GLOBAL_PLACEHOLDER_ORG_ID, AuthzId, Data),
    ObjWithDate = chef_object:set_created(Object, RequesterId),
    ObjWithOldId = ObjWithDate#oc_chef_group{id = Guid},
    ObjWithFakeOrgId = ObjWithOldId#oc_chef_group{org_id = ?GLOBAL_PLACEHOLDER_ORG_ID},
    moser_chef_converter:try_insert(global_placeholder_org, ObjWithFakeOrgId, Guid, AuthzId).

