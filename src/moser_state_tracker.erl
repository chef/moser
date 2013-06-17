%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92 -*-
%% ex: ts=4 sw=4 et
%%
%% @author Marc Paradise <marc@opscode.com>
%% @copyright 2013, Opscode Inc


-module(moser_state_tracker).

-export([capture_full_org_state_list/0,
         capture_full_org_state_list/1,
         add_missing_orgs/0,
         add_missing_orgs/1,
         insert_one_org/1,
         next_ready_org/0,          % Grab next org name waiting to be processed
         next_purge_ready_org/0,    % Grab next org name waiting to be purged
         migration_started/1,       % Update org state to indicate migration started
         migration_failed/2,        % Update org state to indicate migration failed
         migration_successful/1,    % Update org state to indicate migration successful
         hold_migration/1,          % reset org state from ready to holding
         ready_migration/1,         % reset org state from holding to ready
         reset_migration/1,         % reset org state from failed to holding.
         reset_purged_orgs/0,
         reset_purge_started_orgs/0,
         is_ready/1,                % true if org state allows migration,
         org_state/1,               % migration state of the named org
         unmigrated_orgs/0,
         purge_started/1,
         purge_successful/1,
         migrated_orgs/0
        ]).

-include("moser.hrl").

%%% API

%% @doc capture current list of orgs into the sql table
capture_full_org_state_list() ->
    capture_full_org_state_list(moser_acct_processor:open_account()).

capture_full_org_state_list(#account_info{} = AcctInfo) ->
    insert_org(moser_acct_processor:all_orgs(AcctInfo)).


add_missing_orgs() ->
    add_missing_orgs(moser_acct_processor:open_account()).

add_missing_orgs(#account_info{} = AcctInfo) ->
    AllOrgs = moser_acct_processor:all_orgs(AcctInfo),
    %% TODO: add check to insure that this org wasn't created directly in sql
    %% Probably will need to check in with redis to see if it was migrated.
    [ insert_one_org(O) || O <- AllOrgs].

insert_org([]) ->
    ok;
insert_org([Org | T]) ->
    case insert_one_org(Org) of
        ok ->
            insert_org(T);
        {error, Error} ->
            lager:error([{org_name, Org#org_info.org_name}], "Failed to create state record for org. Aborting inserts. Error: ~p", [Error]),
            {error, Error}
    end.

insert_one_org(#org_info{org_id = OrgId, org_name = OrgName}) ->
    case sqerl:execute(insert_org_sql(), [OrgName, OrgId]) of
        {ok, 1} ->
            ok;
        {error, Error} ->
            {error, Error}
    end.



%% @doc true if org state allows it to be migrated
is_ready(OrgName) ->
    is_org_in_state(OrgName, "ready").

is_org_in_state(OrgName, State) ->
    case sqerl:execute(is_org_in_state_sql(), [OrgName, State]) of
        {error, Error} ->
            lager:error("Failed to verify org state: ~p ~p Error: ~p",
                [OrgName, State, Error]),
            {error, Error};
        {ok, Rows} when is_list(Rows) ->
            XF = sqerl_transformers:first_as_scalar(count),
            {ok, Value} = XF(Rows),
            Value > 0
    end.

%% @doc get the next org name to be processed.
next_ready_org() ->
    fetch_orgs(next_org_sql(), ["ready"], {ok, no_more_orgs}, "pending").

%% @doc get the next org name to be processed.
next_purge_ready_org() ->
    fetch_orgs(next_org_sql(), ["completed"], {ok, no_more_orgs}, "completed").

migration_started(OrgName) ->
    update_if_org_in_state(OrgName, start_migration_sql(), "ready", [OrgName]).

migration_failed(OrgName, FailureLocation) ->
    update_if_org_in_state(OrgName, finish_migration_sql(), "started", [OrgName, "failed", FailureLocation]).

migration_successful(OrgName) ->
    update_if_org_in_state(OrgName, finish_migration_sql(), "started", [OrgName, "completed", ""]).

purge_started(OrgName) ->
    update_if_org_in_state(OrgName, finish_migration_sql(), "completed", [OrgName, "purge_started", ""]).

purge_successful(OrgName) ->
    update_if_org_in_state(OrgName, finish_purge_sql(), "purge_started", [OrgName, "purge_successful", ""]).

reset_migration(OrgName) ->
    update_if_org_in_state(OrgName, reset_org_sql(), "failed", [OrgName, "holding"]).

reset_purged_orgs() ->
    [update_if_org_in_state(OrgName, reset_org_sql(), "purge_successful", [OrgName, "holding"]) || OrgName <- purged_orgs()].

reset_purge_started_orgs() ->
    [update_if_org_in_state(OrgName, reset_org_sql(), "purge_started", [OrgName, "holding"]) || OrgName <- purge_started_orgs()].

hold_migration(OrgName) ->
    update_if_org_in_state(OrgName, reset_org_sql(), "ready", [OrgName, "holding"]).

ready_migration(OrgName) ->
    update_if_org_in_state(OrgName, reset_org_sql(), "holding", [OrgName, "ready"]).

update_if_org_in_state(OrgName, SQL, State, Args) ->
    case is_org_in_state(OrgName, State) of
        true ->
            exec_update(SQL, Args);
        false ->
            {error, not_in_expected_state, State}
    end.

exec_update(Query, Params) ->
    case sqerl:execute(Query, Params) of
        {ok, _Num} ->
            ok;
        {error, Error} ->
            lager:error("UPDATE FAILED: ~p  ERROR: ~p", [Query, Error]),
            {error, Error}
    end.

org_state(OrgName) ->
    case sqerl:execute(org_state_sql(), [OrgName]) of
        {error, Error} ->
            lager:error("Failed to fetch next pending org: ~p", [Error]),
            {error, Error};
        {ok, []} ->
            no_such_org;
        {ok, Rows } when is_list(Rows) ->
            XF = sqerl_transformers:first_as_scalar(state),
            {ok, Value} = XF(Rows),
            Value
    end.

unmigrated_orgs() ->
    fetch_orgs(all_unmigrated_orgname_sql(), ["holding", "ready"], no_unmigrated_orgs, "unmigrated").

migrated_orgs() ->
    fetch_orgs(all_orgs_in_state(), ["completed"], no_migrated_orgs, "migrated").

purged_orgs() ->
    fetch_orgs(all_orgs_in_state(), ["purge_successful"], [], "purged").

purge_started_orgs() ->
    fetch_orgs(all_orgs_in_state(), ["purge_started"], [], "purge started").

%%
%% SQL Statements
%%

insert_org_sql() ->
    <<"INSERT INTO org_migration_state (org_name, org_id) VALUES ($1, $2)">>.

start_migration_sql() ->
    <<"UPDATE org_migration_state
       SET state = 'started', migration_start = CURRENT_TIMESTAMP
       WHERE org_name = $1">>.

finish_migration_sql() ->
    <<"UPDATE org_migration_state
       SET state = $2, fail_location = $3, migration_end = CURRENT_TIMESTAMP
       WHERE org_name = $1">>.

finish_purge_sql() ->
    <<"UPDATE org_migration_state
       SET state = $2, fail_location = $3, migration_end = CURRENT_TIMESTAMP
       WHERE org_name = $1">>.

reset_org_sql() ->
    <<"UPDATE org_migration_state
       SET state = $2, fail_location = NULL,
           migration_start = NULL, migration_end = NULL
       WHERE org_name = $1">>.

all_unmigrated_orgname_sql() ->
    <<"SELECT org_name FROM org_migration_state WHERE state = $1 OR state = $2">>.

all_orgs_in_state() ->
    <<"SELECT org_name FROM org_migration_state WHERE state = $1">>.

org_state_sql() ->
    <<"SELECT state FROM org_migration_state WHERE org_name = $1">>.

next_org_sql() ->
    <<"SELECT org_name FROM org_migration_state WHERE state = $1 ORDER BY org_id LIMIT 1">>.

is_org_in_state_sql() ->
    <<"SELECT COUNT(*) count FROM org_migration_state WHERE state = $2 AND org_name = $1">>.

fetch_orgs(SQL, Params, EmptyReturn, ErrorDescription) ->
    case sqerl:execute(SQL, Params) of
        {error, Error} ->
            lager:error("Failed to fetch ~p orgname ~p", [ErrorDescription, Error]),
            {error, Error};
        {ok, []} ->
            EmptyReturn;
        {ok, Rows } when is_list(Rows) ->
            XF = sqerl_transformers:rows_as_scalars(org_name),
            {ok, Value} = XF(Rows),
            Value
    end.
