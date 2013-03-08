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
%% Created :  2013 01 27 by Mark Anderson <>
%%-------------------------------------------------------------------
-module(moser_converter).

%% API
-export([get_chef_list/0,
         file_list_to_orginfo/1,
         filter_out_precreated_orgs/1,
         process_insert_org/1,
         process_insert_orgs/1,
         process_file_list/1,
         process_insert_file/1,
         remove_precreated_from_file_list/1,
         get_couch_path/0]).

-include("moser.hrl").

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% API
%%%===================================================================
get_couch_path() ->
    envy:get(moser, couch_path,
             "/srv/piab/mounts/moser/", %% TODO make default for private chef or something...
             string).

get_chef_list() ->
    Path = get_couch_path(),
    DbPattern = filename:join([Path, "chef_*.couch"]),
    filelib:wildcard(DbPattern).

expand_org_info(Org) ->
    try
        moser_acct_processor:expand_org_info(Org)
    catch
        error:Why ->
            lager:error(?LOG_META(Org), "ORG WILL BE SKIPPED; expand_org_info failed: ~p", [Why]),
            []
    end.

file_list_to_orginfo(L) ->
    lists:flatten([expand_org_info(#org_info{db_name = F}) || F <- L]).

filter_out_precreated_orgs(OL) ->
    [O || O <- OL, not O#org_info.is_precreated].

process_insert_org(OrgInfo) ->
    Start = os:timestamp(),
    case moser_chef_processor:process_couch_file(OrgInfo) of
        {ok, OrgInfoFull} ->
            R = try
                    moser_chef_converter:insert(OrgInfoFull)
                catch
                    error:E ->
                        {error, E, erlang:get_stacktrace()};
                    throw:E ->
                        {error, E, erlang:get_stacktrace()}
                after
                    moser_chef_processor:cleanup_org_info(OrgInfoFull)
                end,
            Time = moser_utils:us_to_secs(timer:now_diff(os:timestamp(), Start)),
            lager:info(?LOG_META(OrgInfo), "COMPLETED ~.3f secs", [Time]),
            R;
        {error, Msg} ->
            {error, Msg}
    end.

process_insert_orgs(L) ->
    [ process_insert_org(O) || O <- L].

%% Deprecated

process_file_list(FileList) ->
    [ process_insert_file(File) || File <- FileList].

remove_precreated_from_file_list(FileList) ->
    AcctInfo = moser_acct_processor:open_account(),
    [ F || F <- FileList,
           not is_file_precreated_org(F, AcctInfo) ].

is_file_precreated_org(File, AcctInfo) ->
    OrgId = moser_utils:get_orgid_from_dbname(File),
    moser_acct_processor:is_precreated_org(OrgId, AcctInfo).

process_insert_file(File) ->
    Start = os:timestamp(),
    {ok, Db} = moser_chef_processor:process_couch_file(File),
    R = try
            moser_chef_converter:insert(Db)
        catch
            error:E ->
                {error, E}
        after
            moser_chef_processor:cleanup_org_info(Db)
        end,
    Time = moser_utils:us_to_secs(timer:now_diff(os:timestamp(), Start)),
    lager:info(?LOG_META(Db), "total time: ~.3f secs", [Time]),
    R.
