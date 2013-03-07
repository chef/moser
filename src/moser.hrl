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

%%%===================================================================
%%% API
%%%===================================================================

-record(moser_config,
        { couchdb_path :: string(),
          couchdb_uri :: string()
        }).

-record(org_info,
        { org_name = undefined,
          org_id = undefined,
          db_name = undefined,
          is_precreated = false,
          chef_ets,
          auth_ets,
          account_info = undefined,
          start_time}).

-record(account_info,
        { user_to_authz,
          authz_to_user,
          orgname_to_guid,
          orgs_by_guid,
          db
        }).

%% we need this as a macro because lager uses a parse transform and can't handle a function
%% call that returns the metadata proplist :(
-define(LOG_META(O),
        [{org_name, binary_to_list(O#org_info.org_name)},
         {org_id, binary_to_list(O#org_info.org_id)}]).
