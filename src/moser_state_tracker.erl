%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92 -*-
%% ex: ts=4 sw=4 et
%%
%% @author Marc Paradise <marc@opscode.com>
%% @copyright 2013, Opscode Inc


-module(moser_state_tracker).

-export([capture_full_org_state_list/0,
         capture_full_org_state_list/1,
         next_ready_org/0,          % Grab next org name waiting to be processed
         migration_started/1,       % Update org state to indicate migration started
         migration_failed/2,        % Update org state to indicate migration failed
         migration_successful/1,    % Update org state to indicate migration successful
         hold_migration/1,          % Prevent named org from being migrated until marked ready
         ready_migration/1,         % Mark named org as ready to migrate,
         is_ready/1,                % true if org state allows migration,
         org_state/1                % migration state of the named org
        ]).

-include("moser.hrl").

%%% API

%% @doc capture current list of orgs into the sql table
capture_full_org_state_list() ->
    capture_full_org_state_list(moser_acct_processor:open_account()).

capture_full_org_state_list(#account_info{} = AcctInfo) ->
    insert_org(moser_acct_processor:all_orgs(AcctInfo)).

insert_org([]) ->
    ok;
insert_org([#org_info{org_id = OrgId, org_name = OrgName} | T]) ->
    case sqerl:execute(insert_org_sql(), [OrgName, OrgId]) of
        {ok, 1} ->
            insert_org(T);
        {error, Error} ->
            lager:error([{org_name, OrgName}], "Failed to create state record for org. Aborting inserts. Error: ~p", [Error]),
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
    case sqerl:execute(next_org_sql(), ["ready"]) of
        {error, Error} ->
            lager:error("Failed to fetch next pending org: ~p", [Error]),
            {error, Error};
        {ok, []} ->
            no_more_orgs;
        {ok, Rows } when is_list(Rows) ->
            XF = sqerl_transformers:first_as_scalar(org_name),
            XF(Rows)
    end.

migration_started(OrgName) ->
    update_if_org_in_state(OrgName, start_migration_sql(), "ready", [OrgName]).

migration_failed(OrgName, FailureLocation) ->
    update_if_org_in_state(OrgName, finish_migration_sql(), "started", [OrgName, "failed", FailureLocation]).

migration_successful(OrgName) ->
    update_if_org_in_state(OrgName, finish_migration_sql(), "started", [OrgName, "completed", ""]).

update_if_org_in_state(OrgName, SQL, State, Args) ->
    case is_org_in_state(OrgName, State) of
        true ->
            exec_update(SQL, Args);
        false ->
            {error, not_in_expected_state, State}
    end.

hold_migration(OrgName) ->
    exec_update(reset_org_sql(), [OrgName, "holding"]).

ready_migration(OrgName) ->
    exec_update(reset_org_sql(), [OrgName, "ready"]).

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

reset_org_sql() ->
    <<"UPDATE org_migration_state
       SET state = $2, fail_location = NULL,
           migration_start = NULL, migration_end = NULL
       WHERE org_name = $1">>.

org_state_sql() ->
    <<"SELECT state FROM org_migration_state WHERE org_name = $1">>.

next_org_sql() ->
    <<"SELECT org_name FROM org_migration_state WHERE state = $1 ORDER BY org_id LIMIT 1">>.

is_org_in_state_sql() ->
    <<"SELECT COUNT(*) count FROM org_migration_state WHERE state = $2 AND org_name = $1">>.

