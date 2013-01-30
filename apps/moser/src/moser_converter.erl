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
         process_file_list/1,
         process_insert_file/1,
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

process_file_list(FileList) ->
    [ process_insert_file(File) || File <- FileList].

process_insert_file(File) ->
    Db = moser_chef_processor:process_couch_file(File),
    try
        moser_chef_converter:insert(Db)
    catch
        error:E ->
            {error, E}
    after
        moser_chef_processor:cleanup_org_info(Db)
    end.
