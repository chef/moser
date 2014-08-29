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
-export([all_orgs/1,
         open_account/0,
         close_account/1,
         process_account_file/0,
         cleanup_account_info/1,
         user_to_auth/3,
         get_org_guid_by_name/2,
         get_org_by_guid/2,
         get_global_containers_list/1,
         get_global_groups_list/1,
         is_precreated_org/1,
         is_precreated_org/2,
         expand_org_info/1,
         get_parsed_org_object_by_name/2,
         all_org_association_data/2
        ]).

-include_lib("moser/include/moser.hrl").

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% API
%%%===================================================================

open_account() ->
    open_account([{access, read}]).

open_account(Args) ->
    {ok, U2A} = moser_utils:dets_open_file(user_to_authz, Args),
    {ok, A2U} = moser_utils:dets_open_file(authz_to_user, Args),
    {ok, O2G} = moser_utils:dets_open_file(orgname_to_guid, Args),
    {ok, OrgIdToGuid} = moser_utils:dets_open_file(org_id_to_guid, Args),
    {ok, Orgs} = moser_utils:dets_open_file(orgs_by_guid, Args),
    {ok, Db}  = moser_utils:dets_open_file(account_db, Args),
    {ok, Containers}  = moser_utils:dets_open_file(global_containers, Args),
    {ok, Groups}  = moser_utils:dets_open_file(global_groups, Args),
    {ok, OrgUserAssoc}  = moser_utils:dets_open_file(org_user_associations, Args),
    {ok, AssociationRequests}  = moser_utils:dets_open_file(association_requests, Args),
    #account_info{
                   user_to_authz = U2A,
                   authz_to_user = A2U,
                   orgname_to_guid = O2G,
                   org_id_to_guid = OrgIdToGuid,
                   orgs_by_guid = Orgs,
                   db = Db,
                   global_containers = Containers,
                   global_groups = Groups,
                   org_user_associations = OrgUserAssoc,
                   association_requests = AssociationRequests,
                   couch_cn = chef_otto:connect()
                 }.

close_account(#account_info{user_to_authz = U2A,
                            authz_to_user = A2U,
                            orgname_to_guid = O2G,
                            org_id_to_guid = OrgIdToGuid,
                            orgs_by_guid = Orgs,
                            global_containers = Containers,
                            global_groups = Groups,
                            org_user_associations = OrgUserAssoc,
                            association_requests = AssociationRequests,
                            db = Db}) ->
    dets:close(U2A),
    dets:close(A2U),
    dets:close(O2G),
    dets:close(OrgIdToGuid),
    dets:close(Orgs),
    dets:close(Containers),
    dets:close(Groups),
    dets:close(OrgUserAssoc),
    dets:close(AssociationRequests),
    dets:close(Db).

cleanup_account_info(#account_info{user_to_authz = U2A,
                                   authz_to_user = A2U,
                                   orgname_to_guid = O2G,
                                   orgs_by_guid = Orgs,
                                   global_containers = Containers,
                                   global_groups = Groups,
                                   org_user_associations = OrgUserAssoc,
                                   association_requests = AssociationRequests,
                                   db = Db}) ->
    dets:delete(U2A),
    dets:delete(A2U),
    dets:delete(O2G),
    dets:delete(Orgs),
    dets:delete(Containers),
    dets:delete(Groups),
    dets:delete(OrgUserAssoc),
    dets:delete(AssociationRequests),
    dets:delete(Db).

%% This is now intended to be called only from within mover_manager.
%% To create the account dets file, please call mover_manager:create_account_dets().
process_account_file() ->
    Account = open_account([{access, read_write}]),
    DbName = filename:join([moser_converter:get_couch_path(), "opscode_account.couch"]),

    IterFn = fun(Key, RevId, Body, AccIn) ->
                     process_account_item(Account, Key, RevId, Body),
                     AccIn
             end,
    decouch_reader:open_process_all(DbName, IterFn),
    close_account(Account).


process_account_item(Account, Key, RevId, Body) ->
    Type = moser_chef_processor:extract_type(Key, Body),
    case Type of
        undefined ->
            %% if it isn't a document type we recognize, we don't care and carry on.
            ignored;
        _ ->
            process_item_by_type(Type, Account, Key, RevId, Body)
    end,
    ok.

process_item_by_type({auth, group},
                     #account_info{global_groups=Groups}, Key, RevId, Body) ->
    dets:insert(Groups, {Key, Body, RevId}),
    ok;
process_item_by_type(auth_join,
                     #account_info{user_to_authz=User2Auth,
                                   authz_to_user=Auth2User},
                     Key, RevId, Body) ->
    UserId = ej:get({<<"user_object_id">>}, Body),
    AuthId = ej:get({<<"auth_object_id">>}, Body),
    dets:insert(User2Auth, {UserId, AuthId, RevId, Key}),
    dets:insert(Auth2User, {AuthId, UserId, RevId, Key}),
    ok;
process_item_by_type(auth_org,
                     #account_info{orgname_to_guid=OrgName2Guid,
                                   orgs_by_guid=Orgs,
                                   org_id_to_guid=OrgIdToGuid},
                     Key, RevId, Body) ->
    OrgName = ej:get({<<"name">>}, Body),
    Guid = ej:get({<<"guid">>}, Body),
    dets:insert(OrgName2Guid, {OrgName, Guid, RevId, Key}),
    dets:insert(Orgs, {Guid, Body, RevId, Key}),
    dets:insert(OrgIdToGuid, {Key, Guid}),
    ok;
process_item_by_type(auth_user,
                     #account_info{db=Db}, Key, RevId, Body) ->
    dets:insert(Db, {{auth_user, Key}, Body, RevId}),
    ok;
process_item_by_type(association_request,
                     #account_info{association_requests=AssociationRequests}, Key, RevId, Body) ->
    dets:insert(AssociationRequests, {Key, Body, RevId}),
    ok;
process_item_by_type(org_user,
                     #account_info{org_user_associations=OrgUserAssoc}, Key, RevId, Body) ->
    dets:insert(OrgUserAssoc, {Key, Body, RevId}),
    ok;
process_item_by_type(design_doc,
                     #account_info{db=Db}, Key, RevId, Body) ->
    dets:insert(Db, {{design_doc, Key}, Body, RevId}),
    ok;
process_item_by_type({auth, container},
                     #account_info{global_containers=Containers}, Key, RevId, Body) ->
    dets:insert(Containers, {Key, Body, RevId}),
    ok;
process_item_by_type(_Type, _Acct, _Key, _RevId, _Body) ->
    %% happily ignore unknown types
    ok.

user_to_auth(_, bad_id, _LogContext) ->
    <<"bad_authz_id">>;
user_to_auth(#account_info{user_to_authz = U2A, couch_cn = Cn}, Id, LogContext) ->
    case dets:lookup(U2A, Id) of
        [{Id,V, _, _}] -> {ok, V};
        [] -> user_to_auth_live(Cn, Id, LogContext)
    end.

user_to_auth_live(Cn, Id, LogContext) ->
    lager:warning(LogContext, "Fallback lookup of user-side id: ~s", [Id]),
    case chef_otto:fetch_auth_join_id(Cn, Id, user_to_auth) of
        {not_found, missing} ->
            {fail, authz_id_not_found};
        {not_found, Why } ->
            {fail, {authz_id_not_found, Why}};
        AuthId ->
            {ok, AuthId}
    end.

%% Method for parsing either org_user or association_request data
%% into a format that those migrations can use.
%%
%% Input:
%%   #account_info: filled out #account_info to access the dets info
%%   atom: (org_user | association_request)
%% Output:
%%   list of all objects found for input atom type: [{UserId, OrgId, LastUpdatedBy, DataBody}, ...]
all_org_association_data(#account_info{org_user_associations = OrgUserAssoc,
                                       association_requests=AssociationRequests,
                                       org_id_to_guid = OrgIdToGuid}, FetchType) ->
    %% Since we didn't store the "last updated by" authz id in couch and
    %% that field is required in SQL, we have to fill in something while migrating.
    %% We are just using pivotal's authz id for this.
    {ok, {user, LastUpdatedBy}} = chef_sql:fetch_object([<<"pivotal">>], user, find_user_by_username, [authz_id]),

    Fun = fun({_Key, UserBody, _RevId}, Acc) ->
                  OrgId = ej:get({<<"organization">>}, UserBody),
                  try
                      [{_, OrgGuid}] = dets:lookup(OrgIdToGuid, OrgId),
                      UserId = ej:get({<<"user">>}, UserBody),
                      [{UserId, OrgGuid, LastUpdatedBy, UserBody} | Acc]
                  catch
                      %% If we can't find a GUID for the Org ID, then this association is bogus.
                      %% Most likely, the org was deleted and not properly cleaned up.
                      _:_ ->
                          lager:warning("org_user_association_warning invalid association for Org ID: ~s ~n", [OrgId]),
                          Acc
                  end

          end,
    case FetchType of
        org_user ->
            DetsTable = OrgUserAssoc;
        association_request ->
            DetsTable = AssociationRequests
    end,
    dets:foldl(Fun, [], DetsTable).

%% @doc Return a list of `org_info' records for all assigned orgs.
all_orgs(#account_info{} = AcctInfo) ->
    WantPrecreated = false,
    all_orgs(AcctInfo, WantPrecreated).

%% @doc Return a complete list of `org_info' records for all orgs found in the account
%% db. If `WantPrecreated' is `true' then unassigned precreated orgs will be included in the
%% list. When this value is `false', only assigned orgs will be included. Assigned orgs are
%% those with a `full_name' field that is not `Pre-created'.
all_orgs(#account_info{orgs_by_guid = Orgs} = AcctInfo, WantPrecreated) ->
    Fun = case WantPrecreated of
              true ->
                  fun({Id, OrgData, _, _}, Acc) ->
                          Rec = org_ejson_to_rec(Id, OrgData, AcctInfo),
                          [Rec | Acc] end;
              false ->
                  fun({Id, OrgData, _, _}, Acc) ->
                          case is_precreated_org(OrgData) of
                              true ->
                                  Acc;
                              false ->
                                  Rec = org_ejson_to_rec(Id, OrgData, AcctInfo),
                                  [Rec | Acc]
                          end
                  end
          end,
    dets:foldl(Fun, [], Orgs).

org_ejson_to_rec(Id, OrgData, AcctInfo) ->
    #org_info{org_name = ej:get({"name"}, OrgData),
              org_id = Id,
              db_name = moser_utils:get_dbname_from_orgid(Id),
              account_info = AcctInfo}.

get_org_guid_by_name(Name,
                     #account_info{orgname_to_guid=OrgName2Guid}) ->
    case dets:lookup(OrgName2Guid, Name) of
        [{Name, GUID, _, _}] ->
            GUID;
        [{Name, GUID}] ->
            GUID;
        [] ->
            not_found
    end.

get_org_by_guid(GUID,
                #account_info{orgs_by_guid=Orgs}) ->
    case dets:lookup(Orgs, GUID) of
        [{GUID, OrgData, _, _}] ->
            OrgData;
        [{GUID, OrgData}] ->
            OrgData;
        [] ->
            not_found
    end.

is_precreated_org({[_H|_T]} = OrgEjson)  ->
    ej:get({"full_name"}, OrgEjson) =:= <<"Pre-created">>.

is_precreated_org(OrgId, AInfo) ->
    is_precreated_org(get_org_by_guid(OrgId, AInfo)).

% returns the org object in a useful format:
% {guid, authz_id, requester_id, raw_object}
get_parsed_org_object_by_name(#account_info{orgs_by_guid=Orgs,
                                            orgname_to_guid=OrgName2Guid,
                                            user_to_authz=User2Auth},
                              OrgName) ->
    [{_, Guid, _, Key}] = dets:lookup(OrgName2Guid, OrgName),
    [{_, RawObject, _, _}] = dets:lookup(Orgs, Guid),
    % update with correct time format
    OldTimestamp = ej:get({<<"assigned_at">>}, RawObject),
    NewTimestamp = list_to_binary(lists:sublist(re:replace(OldTimestamp, "/", "-", [global, {return, list}]), 19)),
    UpdatedRawObject = ej:set({<<"assigned_at">>}, RawObject, NewTimestamp),
    [{_, AuthzId, _, _}] = dets:lookup(User2Auth, Key),
    {Guid,
     AuthzId,
     ej:get({<<"requester_id">>}, UpdatedRawObject),
     UpdatedRawObject
    }.


%% returns a list of [{container_guid, authz_guid, last_requestor_guid, container_name_atom}, ...]
get_global_containers_list(#account_info{global_containers=GlobalContainers,
                                         user_to_authz=UserToAuthz
                                        }) ->
    Fun = fun({Guid, RawObject, _}, Acc) ->
                  RequesterId = ej:get({<<"requester_id">>}, RawObject),
                  [{_, AuthzId, _, _}] = dets:lookup(UserToAuthz, Guid),
                  [{Guid, AuthzId, RequesterId, RawObject} | Acc]
          end,
    dets:foldl(Fun, [], GlobalContainers).

get_global_groups_list(#account_info{global_groups=GlobalGroups,
                                     user_to_authz=UserToAuthz
                                    }) ->
    Fun = fun({Guid, RawObject, _}, Acc) ->
                  RequesterId = ej:get({<<"requester_id">>}, RawObject),
                  try
                      [{_, AuthzId, _, _}] = dets:lookup(UserToAuthz, Guid),
                      [{Guid, AuthzId, RequesterId, RawObject} | Acc]
                  catch
                      %% if you can't find an authz id, log it and continue
                      _:_ ->
                          lager:warning("global_groups no authz id for group GUID: ~s ~n", [Guid]),
                          Acc
                  end
         end,
    dets:foldl(Fun, [], GlobalGroups).

%% FIXME: account_info is really a global tabel and could be extracted out.

%% @doc Returns complete `#org_info{}' record or `not_found' given a partial `#org_info{}'
%% record. You can start with exactly one of the following defined and other two undefined:
%% org_name, org_id, db_name.
expand_org_info(#org_info{account_info = undefined} = OrgInfo) ->
    %% ensure the global account_info has been opened. The open call behaves idempotently.
    AcctInfo = moser_acct_processor:open_account(),
    expand_org_info(OrgInfo#org_info{account_info = AcctInfo});
expand_org_info(#org_info{org_name = OrgName, org_id = undefined, db_name = undefined,
                          account_info = #account_info{} = Acct} = OrgInfo) ->
    %% Given OrgName...
    case get_org_guid_by_name(OrgName, Acct) of
        not_found ->
            not_found;
        OrgId ->
            OrgDesc = get_org_by_guid(OrgId, Acct),
            DbName = moser_utils:get_dbname_from_orgid(OrgId),
            OrgInfo#org_info{org_id = OrgId,
                             db_name = DbName,
                             is_precreated = is_precreated_org(OrgDesc)}
    end;
expand_org_info(#org_info{org_name = undefined, org_id = OrgId, db_name = undefined,
                          account_info = #account_info{} = Acct} = OrgInfo) ->
    %% Given OrgId...
    case get_org_by_guid(OrgId, Acct) of
        not_found ->
            not_found;
        OrgDesc ->
            OrgName = ej:get({"name"}, OrgDesc),
            DbName = moser_utils:get_dbname_from_orgid(OrgId),
            OrgInfo#org_info{org_name = OrgName,
                             db_name = DbName,
                             is_precreated = is_precreated_org(OrgDesc)}
    end;
expand_org_info(#org_info{org_name = undefined, org_id = undefined, db_name = DbName,
                          account_info = #account_info{} = Acct} = OrgInfo) ->
    %% Given chef_* couchdb file path
    OrgId = moser_utils:get_orgid_from_dbname(DbName),
    case get_org_by_guid(OrgId, Acct) of
        not_found ->
            not_found;
        OrgDesc ->
            OrgName = ej:get({"name"}, OrgDesc),
            OrgInfo#org_info{org_name = OrgName,
                             org_id = OrgId,
                             is_precreated = is_precreated_org(OrgDesc)}
    end.

