%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92 -*-
%% ex: ts=4 sw=4 et
%%
%% @author Marc Paradise <marc@opscode.com>
%% @copyright 2013, Opscode Inc


-module(moser_state_tracker).

-export([init_cache/0,
         capture_full_org_state_list/0,
         capture_full_org_state_list/1,
         add_missing_orgs/0,
         add_missing_orgs/1,
         insert_one_org/1,
         insert_one_org/2,
         build_validation_table/0,
         validation_results/1,
         validation_started/1,
         validation_completed/2,
         validation_failed/2,
         next_validation_ready_org/0,
         next_ready_org/0,          % Grab next org name waiting to be processed
         next_ready_org/1,
         next_purge_ready_org/0,    % Grab next org name waiting to be purged
         migration_started/1,       % Update org state to indicate migration started
         migration_started/2,
         migration_failed/2,        % Update org state to indicate migration failed
         migration_failed/3,
         migration_successful/1,    % Update org state to indicate migration successful
         migration_successful/2,
         hold_migration/1,          % reset org state from ready to holding
         hold_migration/2,
         ready_migration/1,         % reset org state from holding to ready
         ready_migration/2,
         reset_migration/1,         % reset org state from failed to holding.
         reset_migration/2,
         reset_purged_orgs/0,
         reset_purged_org_to_completed/1,
         reset_purge_started_orgs/0,
         reset_purge_started_org_to_completed/1,
         is_ready/1,                % true if org state allows migration,
         is_ready/2,
         org_state/1,               % migration state of the named org
         org_state/2,
         org_id_from_name/1,
         all_migrated_orgs/0,
         all_migrated_orgs/1,
         unmigrated_orgs/0,
         unmigrated_orgs/1,
         purge_started/1,
         purge_successful/1,
         migrated_orgs/0,
         migrated_orgs/1,
         force_org_to_state/3
        ]).

-include_lib("moser/include/moser.hrl").

-define(PHASE_ONE_MIGRATION_NAME, mover_phase_1_migrator_callback:migration_type()).
-define(PURGE_MIGRATION_NAME, <<"purge_migration">>).

%%% API

force_org_to_state(OrgName, MigrationType, State) ->
    exec_update(reset_org_sql(), [OrgName, State, MigrationType]).

init_cache() ->
    ets:new(org_name_id_cache, [public, named_table]).

%% @doc capture current list of orgs into the sql table
capture_full_org_state_list() ->
    capture_full_org_state_list(moser_acct_processor:open_account()).

capture_full_org_state_list(#account_info{} = _AcctInfo) ->
    {error, {org_capture_disabled, "Use moser_state_tracker:insert_one_org/1 for specific orgs instead"}}.

add_missing_orgs() ->
    add_missing_orgs(moser_acct_processor:open_account()).

add_missing_orgs(#account_info{} = _AcctInfo) ->
    {error, {org_capture_disabled, "Use moser_state_tracker:insert_one_org/1 for specific orgs instead"}}.

insert_one_org(Org) ->
    insert_one_org(Org, ?PHASE_ONE_MIGRATION_NAME).

insert_one_org( #org_info{org_id = OrgId, org_name = OrgName}, MigrationType) ->
    case sqerl:execute(insert_org_sql(), [OrgName, OrgId, MigrationType]) of
        {ok, 1} ->
            ok;
        {error, Error} ->
            {error, Error}
    end.

build_validation_table() ->
    {ok, OrgVal} = moser_utils:dets_open_file(org_val_state),
    dets:delete_all_objects(OrgVal),
    AllOrgs = all_migrated_orgs(),
    [dets:insert(OrgVal, {OrgName, pending, []}) || OrgName <- AllOrgs],
    dets:close(OrgVal).

next_validation_ready_org() ->
    {ok, OrgVal} = moser_utils:dets_open_file(org_val_state),
    % find the first record with result 'pending' and update it to
    % 'validating'
    MatchSpec = { '$1', pending, '_' },
    case dets:match(OrgVal, MatchSpec, 1) of
        '$end_of_table' ->
            {ok, no_more_orgs};
        { [[]], _ } ->
            i_should_not_happen;
        { [H|_], _ } ->
            [H1|_] = H,
            H1
    end.

validation_started(OrgName) ->
    {ok, OrgVal} = moser_utils:dets_open_file(org_val_state),
    case validation_results(OrgName) of
        org_not_available ->
            {error, org_not_available};
        _ ->
            dets:insert(OrgVal, {OrgName, validating, []})
    end.

validation_failed(OrgName, ErrorResults) ->
    {ok, OrgVal} = moser_utils:dets_open_file(org_val_state),
    case validation_results(OrgName) of
        org_not_available ->
            {error, org_not_available};
        _ ->
            dets:insert(OrgVal, {OrgName, fatal_error, ErrorResults})
    end.

validation_completed(OrgName, Results) ->
    {ok, OrgVal} = moser_utils:dets_open_file(org_val_state),
    case validation_results(OrgName) of
        org_not_available ->
            {error, org_not_available};
        _ ->
            dets:insert(OrgVal, {OrgName, complete, Results})
    end.

validation_results(OrgName) ->
    {ok, OrgVal} = moser_utils:dets_open_file(org_val_state),
    case dets:lookup(OrgVal, OrgName) of
        [] ->
            org_not_available;
        [{OrgName, State, Results}] ->
            {State, Results}
    end.

%% @doc true if org state allows it to be migrated
is_ready(OrgName) ->
    is_ready(OrgName, ?PHASE_ONE_MIGRATION_NAME).

is_ready(OrgName, MigrationType) ->
    is_org_in_state(OrgName, "ready", MigrationType).

is_org_in_state(OrgName, State, MigrationType) ->
    case sqerl:execute(is_org_in_state_sql(), [OrgName, State, MigrationType]) of
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
    next_ready_org(?PHASE_ONE_MIGRATION_NAME).

next_ready_org(MigrationType) ->
    fetch_orgs(next_org_sql(), ["ready", MigrationType], {ok, no_more_orgs}, "pending").

%% @doc get the next org name to be processed.
next_purge_ready_org() ->
    next_ready_org(?PURGE_MIGRATION_NAME).

migration_started(OrgName) ->
    migration_started(OrgName, ?PHASE_ONE_MIGRATION_NAME).

migration_started(OrgName, MigrationType) ->
    update_if_org_in_state(OrgName, MigrationType, start_migration_sql(), "ready", [OrgName]).

migration_failed(OrgName, FailureLocation) ->
    migration_failed(OrgName, FailureLocation, ?PHASE_ONE_MIGRATION_NAME).

migration_failed(OrgName, FailureLocation, MigrationType) ->
    update_if_org_in_state(OrgName, MigrationType, finish_migration_sql(), "started", [OrgName, "failed", FailureLocation]).

migration_successful(OrgName) ->
    migration_successful(OrgName, ?PHASE_ONE_MIGRATION_NAME).

migration_successful(OrgName, MigrationType) ->
    update_if_org_in_state(OrgName, MigrationType, finish_migration_sql(), "started", [OrgName, "completed", ""]).

purge_started(OrgName) ->
    update_if_org_in_state(OrgName, ?PURGE_MIGRATION_NAME, update_state_sql(), "completed", [OrgName, "purge_started"]).

purge_successful(OrgName) ->
    update_if_org_in_state(OrgName, ?PURGE_MIGRATION_NAME, update_state_sql(), "purge_started", [OrgName, "purge_successful"]).

reset_migration(OrgName) ->
    reset_migration(OrgName, ?PHASE_ONE_MIGRATION_NAME).

reset_migration(OrgName, MigrationType) ->
    update_if_org_in_state(OrgName, MigrationType, reset_org_sql(), "failed", [OrgName, "holding"]).

reset_purged_orgs() ->
    [update_if_org_in_state(OrgName, ?PURGE_MIGRATION_NAME, reset_org_sql(), "purge_successful", [OrgName, "holding"]) || OrgName <- purged_orgs()].

reset_purged_org_to_completed(OrgName) ->
    update_if_org_in_state(OrgName, ?PURGE_MIGRATION_NAME, reset_org_sql(), "purge_successful", [OrgName, "completed"]).

reset_purge_started_orgs() ->
    [update_if_org_in_state(OrgName, ?PURGE_MIGRATION_NAME, reset_org_sql(), "purge_started", [OrgName, "holding"]) || OrgName <- purge_started_orgs()].

reset_purge_started_org_to_completed(OrgName) ->
    update_if_org_in_state(OrgName, ?PURGE_MIGRATION_NAME, reset_org_sql(), "purge_started", [OrgName, "completed"]).

hold_migration(OrgName) ->
    hold_migration(OrgName, ?PHASE_ONE_MIGRATION_NAME).

hold_migration(OrgName, MigrationType) ->
    update_if_org_in_state(OrgName, MigrationType, reset_org_sql(), "ready", [OrgName, "holding"]).

ready_migration(OrgName) ->
    ready_migration(OrgName, ?PHASE_ONE_MIGRATION_NAME).

ready_migration(OrgName, MigrationType) ->
    update_if_org_in_state(OrgName, MigrationType, reset_org_sql(), "holding", [OrgName, "ready"]).

update_if_org_in_state(OrgName, MigrationType, SQL, State, Args) ->
    case is_org_in_state(OrgName, State, MigrationType) of
        true ->
            exec_update(SQL, Args ++ [MigrationType]);
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
    org_state(OrgName, ?PHASE_ONE_MIGRATION_NAME).

org_state(OrgName, MigrationType) ->
    case sqerl:execute(org_state_sql(), [OrgName, MigrationType]) of
        {error, Error} ->
            lager:error("Failed to fetch org state org: ~p", [Error]),
            {error, Error};
        {ok, []} ->
            no_such_org;
        {ok, Rows } when is_list(Rows) ->
            XF = sqerl_transformers:first_as_scalar(state),
            {ok, Value} = XF(Rows),
            Value
    end.

org_id_from_name(OrgName) ->
    case ets:lookup(org_name_id_cache, OrgName) of
        [] ->
            OrgId = fetch_org_id_from_name(OrgName),
            ets:insert(org_name_id_cache, {OrgName, OrgId}),
            OrgId;
        [{OrgName, OrgId}] ->
            OrgId
    end.

fetch_org_id_from_name(OrgName) ->
    case sqerl:execute(org_id_from_name_sql(), [OrgName]) of
        {error, Error} ->
            {error, Error};
        {ok, []} ->
            not_found;
        {ok, Rows } when is_list(Rows) ->
            XF = sqerl_transformers:first_as_scalar(org_id),
            {ok, Value} = XF(Rows),
            Value
    end.

%% SQL
org_id_from_name_sql() ->
    <<"SELECT org_id FROM org_migration_state WHERE org_name = $1">>.

all_migrated_orgs() ->
    all_migrated_orgs(?PHASE_ONE_MIGRATION_NAME).

all_migrated_orgs(MigrationType) ->
    org_names_in_states(["completed", "purge_successful", "purge_started"], MigrationType).

unmigrated_orgs() ->
    unmigrated_orgs(?PHASE_ONE_MIGRATION_NAME).

unmigrated_orgs(MigrationType) ->
    org_names_in_states(["holding", "ready"], MigrationType).

migrated_orgs() ->
    migrated_orgs(?PHASE_ONE_MIGRATION_NAME).

migrated_orgs(MigrationType) ->
    org_names_in_states(["completed"], MigrationType).

purged_orgs() ->
    org_names_in_states(["purge_successful"], ?PURGE_MIGRATION_NAME).

purge_started_orgs() ->
    org_names_in_states(["purge_started"], ?PURGE_MIGRATION_NAME).

org_names_in_states(States, MigrationType) ->
    States2 = [list_to_binary(X) || X <- States, is_list(X)],
    case sqerl:adhoc_select([<<"org_name">>], <<"org_migration_state">>,
                            {'and',[{<<"state">>, in, States2}, {<<"migration_type">>, equals, MigrationType}]}) of
        {error, Error} ->
            lager:error("Failed to fetch org names for orgs in state(s): ~p error: ~p", [States, Error]);
        {ok, []} ->
            no_orgs_in_state;
        {ok, Rows} when is_list(Rows) ->
            XF = sqerl_transformers:rows_as_scalars(org_name),
            {ok, Value} = XF(Rows),
            Value
    end.

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

%%
%% SQL Statements
%%

insert_org_sql() ->
    <<"INSERT INTO org_migration_state (org_name, org_id, migration_type) VALUES ($1, $2, $3)">>.

start_migration_sql() ->
    <<"UPDATE org_migration_state
       SET state = 'started', migration_start = CURRENT_TIMESTAMP
       WHERE org_name = $1 AND migration_type = $2">>.

finish_migration_sql() ->
    <<"UPDATE org_migration_state
       SET state = $2, fail_location = $3, migration_end = CURRENT_TIMESTAMP
       WHERE org_name = $1 AND migration_type = $4">>.

update_state_sql() ->
    <<"UPDATE org_migration_state SET state = $2 WHERE org_name = $1 AND migration_type = $3">>.

reset_org_sql() ->
    <<"UPDATE org_migration_state
       SET state = $2, fail_location = NULL,
           migration_start = NULL, migration_end = NULL
       WHERE org_name = $1 AND migration_type = $3">>.

org_state_sql() ->
    <<"SELECT state FROM org_migration_state WHERE org_name = $1 and migration_type = $2">>.

next_org_sql() ->
    <<"SELECT org_name FROM org_migration_state WHERE state = $1 AND migration_type = $2 ORDER BY org_id LIMIT 1">>.

is_org_in_state_sql() ->
    <<"SELECT COUNT(*) count FROM org_migration_state WHERE state = $2 AND org_name = $1 and migration_type = $3">>.

