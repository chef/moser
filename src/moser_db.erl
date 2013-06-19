%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92-*-
%% ex: ts=4 sw=4 et
%%
%% @author Marc Paradise <marc@opscode.com>
%% @copyright 2013, Opscode Inc
%%
%% Tools for chef-object related queries in sql db.  Where appropriate,
%% it acts as a simple wrapper around chef_sql functions that require an
%% org id instead of org name.
%%
%% Note that org migration state queries/sql is managed
%% via moser_state_tracker.
%%
-module(moser_db).

-export([fetch_all_cookbook_version_dependencies/1,
         fetch_nodes/1,
         fetch_node/2,
         fetch_environment/2]).

fetch_all_cookbook_version_dependencies(OrgName) ->
    OrgId = moser_state_tracker:org_id_from_name(OrgName),
    case chef_sql:fetch_all_cookbook_version_dependencies(OrgId) of
        {ok, Results} ->
            Results;
        {error, Error} ->
            {error, Error}
    end.

fetch_nodes(OrgName) ->
    OrgId = moser_state_tracker:org_id_from_name(OrgName),
    case chef_sql:fetch_nodes(OrgId) of
        {ok, L} ->
            L;
        {error, Error} ->
            {error, Error}
    end.

fetch_environment(OrgName, EnvironmentName) ->
    OrgId = moser_state_tracker:org_id_from_name(OrgName),
    case chef_sql:fetch_environment(OrgId, EnvironmentName) of
        {ok, L} ->
            L;
        {error, Error} ->
            {error, Error}
    end.

fetch_node(OrgName, NodeName) ->
    OrgId = moser_state_tracker:org_id_from_name(OrgName),
    case chef_sql:fetch_node(OrgId, NodeName) of
        {ok, L} ->
            L;
        {error, Error} ->
            {error, Error}
    end.

