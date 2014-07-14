%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92 -*-
%% ex: ts=4 sw=4 et
%% @author Tyler Cloke <tyler@getchef.com>
%%
%% Copyright 2014 Chef, Inc. All Rights Reserved.

-module(moser_org_converter).

-include("moser.hrl").
-include_lib("oc_chef_authz/include/oc_chef_types.hrl").

-export([
	 insert_org/4,
	 insert_org_user_association/4,
	 insert_org_user_invite/4
	]).

insert_org(Guid, AuthzId, LastUpdatedBy, RawObject) ->
    Object = chef_object:new_record(oc_chef_organization, null, AuthzId, RawObject),
    ObjWithDate = chef_object:set_created(Object, LastUpdatedBy),
    ObjWithOldId = ObjWithDate#oc_chef_organization{id = Guid},
    moser_chef_converter:try_insert(no_org, ObjWithOldId, Guid, AuthzId).

insert_org_user_association(UserGuid, OrgGuid, LastUpdatedBy, RawObject) ->
    Object = chef_object:new_record(oc_chef_org_user_association, OrgGuid, null, RawObject),
    ObjWithDate = chef_object:set_created(Object, LastUpdatedBy),
    moser_chef_converter:try_insert(no_org, ObjWithDate, UserGuid, null).

insert_org_user_invite(UserGuid, OrgGuid, LastUpdatedBy, RawObject) ->
    Object = chef_object:new_record(oc_chef_org_user_invite, OrgGuid, null, RawObject),
    ObjWithDate = chef_object:set_created(Object, LastUpdatedBy),
    moser_chef_converter:try_insert(no_org, ObjWithDate, UserGuid, null).
