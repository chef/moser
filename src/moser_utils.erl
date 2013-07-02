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
%% @copyright (C) 2013, Opscode Inc.
%% @doc
%%
%% @end
%% Created :  21 Jan 2013 by Mark Anderson <>
%%-------------------------------------------------------------------
-module(moser_utils).

%% API
-export([
         dbfile_exists/1,
         dets_open_file/1,
         fix_chef_id/1,
         get_dbname_from_orgid/1,
         get_org_id/1,
         get_orgid_from_dbname/1,
         list_ej_keys/1,
         load_process_org/4,
         orgname_to_guid/1,
         for_all_orgs/1,
         type_for_object/1,
         us_to_secs/1
        ]).

-include("moser.hrl").


type_for_object({{client, _}, {_, _}}) ->
    client;
type_for_object({{Type, _}, _}) ->
    Type;
type_for_object(_) ->
    unknown_type.

%% TODO: generate org id prefixed ids and wire in solr re-index.  If we modify the id in any
%% way, we have to re-index in solr. We might as well take advantage and generate ids that
%% we think will have better index behavior.
fix_chef_id(X) ->
    binary:replace(X, <<"-">>, <<>>,[global]).

get_org_id(#org_info{org_id = OrgId}) ->
    iolist_to_binary(OrgId).

orgname_to_guid(OrgName) ->
    BinaryOrgName = iolist_to_binary(OrgName),
    [{BinaryOrgName, BinaryGuid, _, _}] = dets:lookup(orgname_to_guid, BinaryOrgName),
    binary_to_list(BinaryGuid).

get_orgid_from_dbname(DbName) ->
    {match, [OrgId]} = re:run(DbName, ".*chef_([[:xdigit:]]*).couch", [{capture, all_but_first, binary}]),
    OrgId.

get_dbname_from_orgid(OrgId) ->
    FileName = iolist_to_binary(["chef_", OrgId, ".couch"]),
    filename:join([moser_converter:get_couch_path(), FileName]).

list_ej_keys({Ej}) ->
    lists:sort([K || {K,_} <- Ej]).

us_to_secs(USecs) ->
    USecs / 1.0E6.

for_all_orgs(Fun) ->
    AcctInfo = moser_acct_processor:open_account(),
    %% this returns all assigned orgs in the acct db.
    AllOrgs = moser_acct_processor:all_orgs(AcctInfo),
    %% filter out, those where we can't find the couchdb file (useful for test scenario
    %% especially where we have a complete acct db, but partial collection of couchdb
    %% files).
    {T, R} = timer:tc(fun() -> [ Fun(O) || O <- AllOrgs, dbfile_exists(O) ] end ),
    {{T/1.0e6/60.0, min}, R}.


load_process_org(#org_info{org_name = OrgName} = OrgInfo,
                 Action, Cleanup,
                 Description) ->
    Start = os:timestamp(),
    case moser_chef_processor:process_couch_file(OrgInfo) of
        {ok, OrgInfoFull} ->
            R = try
                    Action(OrgInfoFull)
                catch
                    error:E ->
                        {error, E, erlang:get_stacktrace()};
                    throw:E ->
                        {error, E, erlang:get_stacktrace()}
                after
                    Cleanup(OrgInfoFull)
                end,
            Time = moser_utils:us_to_secs(timer:now_diff(os:timestamp(), Start)),
            lager:info(?LOG_META(OrgInfo), "~s ~s COMPLETED ~.3f secs", [Description, OrgName, Time]),
            R;
        {error, Msg} ->
            {error, Msg}
    end.


dets_open_file(Name) ->
    Dir = envy:get(moser, moser_dets_dir,
                   "/srv/piab/mounts/moser",
                   string),
    dets:open_file(Name, [{file, filename:join([Dir, atom_to_list(Name)])}]).

dbfile_exists(#org_info{db_name = DbFile}) ->
    filelib:is_file(DbFile).
