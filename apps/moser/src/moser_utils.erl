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
-export([fix_chef_id/1,
         get_org_id/1,
         list_ej_keys/1
        ]).

-include("moser.hrl").

%%
%% TODO: Determine chef id migration strategy
%%
fix_chef_id(X) ->
    binary:replace(X, <<"-">>, <<>>,[global]).

get_org_id(#org_info{org_id = OrgId}) ->
    iolist_to_binary(OrgId).



list_ej_keys({Ej}) ->
    lists:sort([K || {K,_} <- Ej]).

%clear_fields(Fields, Data) ->
%    lists:foldl(fun(E,A) ->
%                        ej:delete({E},A)
%                end,
%                Data,
%                Fields).

