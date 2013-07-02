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
%% Created :  02 July 2013 by Mark Anderson <mark@opscode.com>
%%-------------------------------------------------------------------
-module(moser_chef_validator).

%% API
-export([process_validate_all/0,
         process_validate_org/1,
         validate_all_cookbook_versions/1,
         validate_cookbook_version/2
        ]).
-include_lib("stdlib/include/qlc.hrl").
-include("moser.hrl").
-include_lib("ej/include/ej.hrl").
-include_lib("chef_objects/include/chef_types.hrl").

process_validate_all() ->
    moser_utils:for_all_orgs(fun process_validate_org/1).

process_validate_org(OrgInfo) ->
    moser_utils:load_process_org(OrgInfo,
                                 fun validate_all_cookbook_versions/1,
                                 fun moser_chef_processor:cleanup_org_info/1,
                                 "VALIDATE").

%%
%% Validate version information in couch data.
%%
validate_all_cookbook_versions(#org_info{chef_ets = Chef} = Org) ->
    Query = qlc:q([ Item || {{cookbook_version, _}, _} = Item <- ets:table(Chef)]),
    [ validate_cookbook_version(Org, V) || V <- qlc:eval(Query) ],
    ok.

version_out_of_range(N) when is_list(N) ->
    version_out_of_range(list_to_integer(N));
version_out_of_range(N) when is_integer(N) ->
    N < 0 orelse N >  1 bsl 31.

validate_version(Version, Org, NameVer,  OldId) ->
    case re:run(Version, "(\\d+)\.(\\d+)\.(\\d+)", [{capture, all_but_first, list}]) of
        {match, V} ->
            case [ F || F <- V, version_out_of_range(F) ] of
                [] -> ok;
                _ ->
                    lager:error(?LOG_META(Org), "cookbook_version ~s (~s) ill_formed ~s ~s", [NameVer, OldId, Version])
            end;
        _ ->
            lager:error(?LOG_META(Org), "cookbook_version ~s (~s) ill_formed ~s ~s", [NameVer, OldId, Version])
    end.

validate_cookbook_version(Org, {{cookbook_version, OldId}, Data}) ->
    NameVer = ej:get({<<"name">>}, Data, <<"#Missing!Name-777.777.777">>),
    Version = ej:get({<<"version">>}, Data, <<"777.777.777">>),
    MVersion = ej:get({<<"metadata">>, <<"version">>}, Data, <<"777.777.777">>),

    validate_version(Version, Org, NameVer, OldId),

    case MVersion of
        Version -> ok;
        _ ->
            lager:error(?LOG_META(Org), "cookbook_version ~s (~s) mismatch ~s ~s", [NameVer, OldId, Version, MVersion])
    end.

