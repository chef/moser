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
-module(moser_chef_converter).

%% API
-export([insert/1,
         cleanup_organization/1,
         cleanup_orgid/1,
         cleanup_all/0]).
-include_lib("stdlib/include/qlc.hrl").
-include("moser.hrl").
-include_lib("ej/include/ej.hrl").
-include_lib("chef_objects/include/chef_types.hrl").

-include_lib("eunit/include/eunit.hrl").

-define(SEGMENTS,  [<<"attributes">>,
                    <<"definitions">>,
                    <<"files">>,
                    <<"libraries">>,
                    <<"providers">>,
                    <<"recipes">>,
                    <<"resources">>,
                    <<"root_files">>,
                    <<"templates">> ]).

%% Order is important because of foreign key constraints on
%% cookbook_version_checksums, checksums, cookbook_versions, and cookbooks
-define(SQL_TABLES, ["cookbook_version_checksums",
                     "checksums",
                     "cookbook_versions",
                     "cookbooks",
                     "environments",
                     "roles",
                     "clients",
                     "data_bag_items",
                     "data_bags"]).

-define(DEFAULT_CHECKSUM_BATCH_SIZE, 1000).

insert(#org_info{} = Org) ->
    try
        {Time0, Totals0} = insert_checksums(Org, dict:new()),
        {Time1, Totals1} = insert_databags(Org, Totals0),
        {Time2, _} = insert_objects(Org, Totals1),
        TotalTime = Time0 + Time1 + Time2,
        lager:info(?LOG_META(Org), "inserts complete ~.3f seconds",
                   [moser_utils:us_to_secs(TotalTime)]),
        {ok, TotalTime}
    catch
        error:E ->
            lager:error(?LOG_META(Org), "~p~n~p", [E, erlang:get_stacktrace()]),
            {error, E}
    end.

%%
%% Checksums need to be inserted before other things
%%
insert_checksums(#org_info{chef_ets = Chef} = Org, Totals) ->
    {T, R} = timer:tc(fun() ->
                              OrgId = moser_utils:get_org_id(Org),
                              %% Select the Data element out of the ets field
                              Query = qlc:q([[OrgId,Checksum] || {{checksum, Checksum},_} <- ets:table(Chef)]),
                              Cursor = qlc:cursor(Query),
                              NewTotals = do_insert_checksums(Cursor, Totals),
                              qlc:delete_cursor(Cursor),
                              NewTotals
                      end),
    lager:info(?LOG_META(Org), "checksum_time ~.3f seconds",
               [moser_utils:us_to_secs(T)]),
		{T,R}.

do_insert_checksums(Cursor, Totals) ->
    case qlc:next_answers(Cursor, ?DEFAULT_CHECKSUM_BATCH_SIZE) of
        [] ->
            Totals;
        Answers ->
            NewTotals = bulk_insert_checksums(Answers, Totals),
            do_insert_checksums(Cursor, NewTotals)
    end.

%% Relies on current schema as documented in
%% chef_db/priv/pgsql_statements.config
%%
%% insert_checksum,%<<"INSERT INTO checksums(org_id, checksum)
%%   VALUES ($1, $2)">>
%%
bulk_insert_checksums(CurrentList, Totals) ->
    case sqerl:adhoc_insert(
            <<"checksums">>, %%Table name
            [<<"org_id">>, <<"checksum">>], %%Column Names
            CurrentList, %% Rows
            ?DEFAULT_CHECKSUM_BATCH_SIZE) of
        {ok, InsertedCount} ->
            dict:update_counter("checksums", InsertedCount, Totals);
        Error ->
            throw(Error)
    end.
%%
%% Databags need to be inserted before other things
%%
insert_databags(#org_info{} = Org, Totals) ->
    insert_objects(Org, Totals, fun insert_databag/3, "databag").

insert_databag(Org, {{databag, Id}, Data} = Object, Acc) ->
    Name = name_for_object(Object),
    {ok, _} = chef_data_bag:parse_binary_json(chef_json:encode(Data), create),
    case get_authz_info(Org, databag, Name, Id) of
        {AuthzId, RequesterId} ->
            OrgId = moser_utils:get_org_id(Org),
            DataBag = chef_object:new_record(chef_data_bag, OrgId, AuthzId, Name),
            ObjWithDate = chef_object:set_created(DataBag, RequesterId),
            ObjWithOldId = set_id(ObjWithDate, Id),
            try_insert(Org, ObjWithOldId, Id, AuthzId, fun chef_sql:create_data_bag/1),
            dict:update_counter(databag, 1, Acc);
        not_found ->
            %% ignore object if authz id not found
            Acc
    end;
insert_databag(_Org, {{_Type, _Id}, _Data} = _Item, Acc) ->
%%    RType = list_to_atom("SKIP_DB_" ++ atom_to_list(_Type)),
%%    dict:update_counter(RType, 1, Acc);
    Acc;
insert_databag(_Org, {orgname,_}, Acc) ->
    Acc;
insert_databag(_Org, Item, Acc) ->
    lager:warning("unexpected item in insert_databag ~p", [Item]),
    Acc.

insert_objects(#org_info{org_name = OrgName,
                         org_id = OrgId,
                         chef_ets = Chef} = Org,
               Totals, InsertFun, Type) ->

    Inserter = fun(Item, Acc) ->
                       try
                           InsertFun(Org, Item, Acc)
                       catch
                           throw:{EType, EDetail} ->
                               maybe_log_throw(Org, Item, EType, EDetail),
                               Acc;
                           throw:#ej_invalid{msg = Msg, type = SpecType, found = Found, key = Key} ->
                               RealType = moser_utils:type_for_object(Item),
                               Props = [{error_type, {RealType, SpecType, Key, Found}}| ?LOG_META(Org)],
                               lager:error(Props, "FAILED ~p ~p",
                                           [Msg, Item]),
                               Acc;
                           Error:Why ->
                               maybe_log_error(Org, Item, Error, Why),
                               Acc
                       end
               end,
    {Time, Totals1} = timer:tc(fun() -> ets:foldl(Inserter, Totals, Chef) end),
    lager:info(?LOG_META(Org), "~p (~p) Insert ~s Stats: ~p~n",
               [OrgName, OrgId, Type, lists:sort(dict:to_list(Totals1))]),
    lager:info(?LOG_META(Org), "~p (~p) ~s insertions took ~.3f seconds~n",
               [OrgName, OrgId, Type, moser_utils:us_to_secs(Time)]),
    {Time, Totals1}.

maybe_log_throw(Org, Item, EType, EDetail) ->
    RealType = moser_utils:type_for_object(Item),
    Props = [{error_type, {RealType, EType}} | ?LOG_META(Org)],
    lager:error(Props, "FAILED ~p ~p ~p",
                [EDetail, Item, erlang:get_stacktrace()]),
    DbErrorIsFatal = envy:get(moser, db_error_is_fatal, true, bool),
    case EType of
        chef_sql when DbErrorIsFatal ->
            %% We want to do a hard failure for database errors
            throw({EType, EDetail});
        _ ->
            ok
    end.

maybe_log_error(Org, Item, Error, Why) ->
    RealType = moser_utils:type_for_object(Item),
    lager:error(?LOG_META(Org), "~s FAILED ~p ~p ~p ~p",
                [RealType,
                 Error, Why, Item, erlang:get_stacktrace()]),
    case envy:get(moser, other_error_is_fatal, true, bool) of
        true ->
            error(Why);
        _ ->
            ok
    end.


insert_objects(#org_info{} = Org, Totals) ->
    insert_objects(Org, Totals, fun insert_one/3, "object").

%% @doc Return the name of a Chef object given `{{Type, IdOrName}, Data}'.  The `Data' value
%% will either be the object EJSON or for clients `{Id, Ejson}'.
name_for_object({{client, Name}, {_Id, _Data}}) ->
    Name;
name_for_object({{databag_item, _Id}, Data}) ->
    ej:get({"data_bag"}, Data);
name_for_object({{Type, _Id}, Data}) when Type =:= role;
                                          Type =:= environment;
                                          Type =:= databag ->
    ej:get({"name"}, Data);
name_for_object({{cookbook_version, _Id}, Data}) ->
    ej:get({"cookbook_name"}, Data);
name_for_object({{_Type, _Id}, _Data}) ->
    unset_name.

%% Return the object id from the object tuple. This is needed because clients are special.
id_for_object({{client, _Name}, {Id, _}}) ->
    Id;
id_for_object({{_Type, Id}, _}) ->
    Id.

%% Set the object ID for a chef-object-style record. This is needed because we will not be normalizing
%% the IDs at this time due to complexity related to search and SOLR indexing.
set_id(#chef_role{} = Object, Id) ->
    Object#chef_role{id = Id};
set_id(#chef_environment{} = Object, Id) ->
    Object#chef_environment{id = Id};
set_id(#chef_client{} = Object, Id) ->
    Object#chef_client{id = Id};
set_id(#chef_data_bag{} = Object, Id) ->
    Object#chef_data_bag{id = Id};
set_id(#chef_data_bag_item{} = Object, Id) ->
    Object#chef_data_bag_item{id = Id}.

insert_one(Org, {{Type, _IdOrName}, _} = Object, Acc) ->
    Name = name_for_object(Object),
    Id = id_for_object(Object),
    case get_authz_info(Org, Type, Name, Id) of
        {AuthzId, RequesterId} ->
            insert_one(Org, Object, AuthzId, RequesterId, Acc);
        not_found ->
            %% ignore object with missing authz id since such an object cannot be accessed
            %% prior to migration.
            Acc
    end;
insert_one(_Org, {orgname, _}, Acc) ->
    %% orgs are ignored for now
    Acc;
insert_one(_Org, Item, Acc) ->
    %% ignore, but log other unhandled items
    lager:warning("unexpected item in insert_one ~p", [Item]),
    Acc.

insert_one(#org_info{org_name = OrgName} = Org,
           {{client, Name}, {OldId, Data}},
           AuthzId, RequesterId, Acc) ->
    {ok, ClientData0} = chef_client:oc_parse_binary_json(chef_json:encode(Data),
                                                         Name, not_found),
    %% assign validator flag based on name: $OrgName-validator
    ClientData = ej:set({<<"validator">>}, ClientData0,
                        is_validator(OrgName, ClientData0)),
    OrgId = moser_utils:get_org_id(Org),
    Client = chef_object:new_record(chef_client, OrgId, AuthzId, ClientData),
    ObjWithDate = chef_object:set_created(Client, RequesterId),
    ObjWithOldId = set_id(ObjWithDate, OldId),
    try_insert(Org, ObjWithOldId, OldId, AuthzId, fun chef_sql:create_client/1),
    dict:update_counter(client, 1, Acc);
insert_one(Org, {{databag_item = Type, OldId}, Data}, _AuthzId, RequesterId, Acc) ->
    BagName = ej:get({<<"data_bag">>}, Data),
    %% returns an unwrapped DBI
    {ok, ItemData} = chef_data_bag_item:parse_binary_json(chef_json:encode(Data), create),
    OrgId = moser_utils:get_org_id(Org),
    DataBagItem = chef_object:new_record(chef_data_bag_item, OrgId, no_authz,
                                         {BagName, ItemData}),
    ObjWithDate = chef_object:set_created(DataBagItem, RequesterId),
    ObjWithOldId = set_id(ObjWithDate, OldId),
    try_insert(Org, ObjWithOldId, OldId, <<"unset">>, fun chef_sql:create_data_bag_item/1),
    dict:update_counter(Type, 1, Acc);
insert_one(Org, {{role, OldId}, Data}, AuthzId, RequesterId, Acc) ->
    %% TODO: a different API in chef_role would eliminate a JSON/EJSON round-trip for
    %% validation and normalization.
    Name = ej:get({"name"}, Data),
    RoleData = soft_validate(role,
                             fun(D) -> chef_role:parse_binary_json(D, create) end,
                             Org, OldId, Data, Name),

    OrgId = moser_utils:get_org_id(Org),
    Role = chef_object:new_record(chef_role, OrgId, AuthzId, RoleData),
    ObjWithDate = chef_object:set_created(Role, RequesterId),
    ObjWithOldId = set_id(ObjWithDate, OldId),
    try_insert(Org, ObjWithOldId, OldId, AuthzId, fun chef_sql:create_role/1),
    dict:update_counter(role, 1, Acc);
%% Environments
insert_one(Org, {{environment, OldId}, Data}, AuthzId, RequesterId, Acc) ->
    OrgId = moser_utils:get_org_id(Org),
    %% first version of environments had a top-level attributes key which is no longer
    %% allowed. If the key is present with an empty value, just remove it.
    Data1 = remove_empty_top_level_attributes(Data),
    {ok, EnvData} = chef_environment:parse_binary_json(chef_json:encode(Data1)),
    Env = chef_object:new_record(chef_environment, OrgId, AuthzId, EnvData),
    ObjWithDate = chef_object:set_created(Env, RequesterId),
    ObjWithOldId = set_id(ObjWithDate, OldId),
    try_insert(Org, ObjWithOldId, OldId, AuthzId, fun chef_sql:create_environment/1),
    dict:update_counter(environment, 1, Acc);
insert_one(Org, {{cookbook_version = Type, OldId}, Data}, AuthzId, RequesterId, Acc) ->
    %% fixup potentially old version constraint strings before inserting into sql
    ConstraintKeys = [<<"dependencies">>,
                      <<"platforms">>,
                      <<"recommendations">>,
                      <<"suggestions">>,
                      <<"conflicting">>,
                      <<"replacing">>,
                      <<"providing">>],
    Fixer = fun(Key, Accum) ->
                    Constraints = ej:get({Key},Accum),
                    Fixed = clean_old_array_dependencies(Constraints),
                    ej:set({Key}, Accum, Fixed)
            end,
    Metadata = ej:get({<<"metadata">>}, Data),
    FixedMeta = lists:foldl(Fixer, Metadata, ConstraintKeys),
    FixedData = ej:set({<<"metadata">>}, Data, FixedMeta),
    %% So cbv data is varied. Our validation function is built around the REST API where the
    %% URL provides name and version. So here we extract two required attributes and do some
    %% munging to get something consistent. If one of the required things is missing, we
    %% should end up with an invalid name, but fail in the validation code to keep messages
    %% consistent.
    NameVer = ej:get({<<"name">>}, FixedData, <<"#Missing!Name-777.777.777">>),
    Version = ej:get({<<"metadata">>, <<"version">>}, FixedData, <<"777.777.777">>),
    Name = re:replace(NameVer, <<"-", Version/binary>>, <<"">>, [{return, binary}]),

    CBVData = soft_validate(cookbook,
                            fun(D) -> chef_cookbook:parse_binary_json(D, {Name, Version}) end,
                            Org, OldId, FixedData, NameVer),

    OrgId = moser_utils:get_org_id(Org),
    CookbookVersion = chef_object:new_record(chef_cookbook_version, OrgId, AuthzId, CBVData),
    ObjWithDate = chef_object:set_created(CookbookVersion, RequesterId),
    case chef_sql:create_cookbook_version(ObjWithDate) of
        {error, invalid_checksum} ->
            %% we'll see an invalid_checksum error if a cookbook_version object contains a
            %% checksum that isn't in the checksums table. This means the cbv is broken
            %% (perhaps as a result of using --purge). So we log and skip these.
            Props = [{error_type, cookbook_version_missing_checksum}| ?LOG_META(Org)],
            lager:warning(Props, "cookbook_version ~s (~s) SKIPPED missing checksums",
                          [ej:get({"name"}, FixedData), OldId]),
            log_insert(skip, Org, OldId, AuthzId, ObjWithDate),
            Acc;
        {ok, 1} ->
            log_insert(ok, Org, OldId, AuthzId, ObjWithDate),
            dict:update_counter(Type, 1, Acc);
        Error ->
            log_insert(fail, Org, OldId, AuthzId, ObjWithDate),
            lager:error(?LOG_META(Org), "cookbook_version ~s (~s) SKIPPED ~p",
                        [ej:get({"name"}, FixedData), OldId, Error]),
            throw({chef_sql, Error})
    end;
%% Old style cookbooks
%% These use the "Cookbook" chef_type. As best we can tell, this isn't used anywhere in the OHC code.
%% May want a final check with Adam and CB to make sure there isn't some secret stuff, but it appears
%% we can ignore them for now.
insert_one(Org, {{cookbook_old = Type, Id}, Data}, _AuthzId, _RequesterId, Acc) ->
    lager:warning(?LOG_META(Org), "cookbook_old ~p", [{Id, Data}]),
    dict:update_counter(Type, 1, Acc);
%% Handled in pass zero
insert_one(_Org, {{checksum, _Id}, _}, _AuthzId, _RequesterId, Acc) ->
    Acc;
insert_one(_Org, {{databag, _Id}, _}, _AuthzId, _RequesterId, Acc) ->
    Acc;
%% Unhandled objects
insert_one(_Org, {{Type, _Id}, _Data} = _Item, _AuthzId, _RequesterId, Acc) ->
    RType = list_to_atom("SKIP_P2_" ++ atom_to_list(Type)),
    dict:update_counter(RType, 1, Acc);
%    Acc;
%% Orgname object should match org name
insert_one(_Org, {orgname, _}, _AuthzId, _RequesterId, Acc) ->
    Acc;
insert_one(Org, Item, _AuthzId, _RequesterId, Acc) ->
    lager:warning(?LOG_META(Org), "unexpected item in insert_one: ~p", [Item]),
    Acc.

%% Almost all of the insert operations call a `chef_sql:create_*(ObjectRec)' function and
%% expect a return of `{ok, 1}'. This function handles this common case and logs details
%% about the insert attempt along with OK/FAIL status.
try_insert(Org, ObjectRec, OldId, AuthzId, Fun) ->
    Status = try Fun(ObjectRec) of
                 {ok, 1} -> ok;
                 Error ->
                     {chef_sql, {Error, OldId, AuthzId}}
             catch
                 EType:Why ->
                     {chef_sql, {{EType, Why}, OldId, AuthzId}}
             end,
    log_insert(Status, Org, OldId, AuthzId, ObjectRec),
    throw_not_ok(Status).

throw_not_ok(ok) ->
    ok;
throw_not_ok(Error) ->
    throw(Error).

log_insert(Status, Org, OldId, AuthzId, ObjectRec) ->
    LogStatus = case Status of
                    ok   -> <<"OK">>;
                    skip -> <<"SKIP">>;
                    _    -> <<"FAIL">>
                end,
    NewId = chef_object:id(ObjectRec),
    Name = name_from_object(ObjectRec),
    Type = chef_object:type_name(ObjectRec),
    lager:info(?LOG_META(Org), "INSERT LOG ~s: ~s ~s ~s ~s ~s",
               [LogStatus, Type, OldId, NewId, AuthzId, Name]),
    Status.

name_from_object(ObjectRec) ->
    case chef_object:name(ObjectRec) of
        {Bag, Item} ->
            <<Bag/binary, ",", Item/binary>>;
        N ->
            N
    end.

%% Intended for use with environment objects, removes a top-level "attributes" key if the
%% value is an empty hash.
remove_empty_top_level_attributes(Data) ->
    case ej:get({<<"attributes">>}, Data) of
        {[]} ->
            ej:delete({<<"attributes">>}, Data);
        _ ->
            Data
    end.

is_validator(OrgName, Data) ->
    %% TODO: the org record in opscode_account specifies the name of the validator; we should modify to use that
    Name = ej:get({"clientname"}, Data),
    Name =:= <<OrgName/binary, "-validator">>.

clean_old_array_dependencies(Deps) ->
    %% {[{<<"akey">>, Constraint}]}
    %% Constraint may be [] or [VC] or [VC1, VC2,...]
    %% Multiple constraints is an error
    %% [VC] -> VC
    %% [] -> ">= 0.0.0"
    {DepList} = Deps,
    {[ {K, clean_constraint(VC)} || {K, VC} <- DepList ]}.

clean_constraint([]) ->
    <<">= 0.0.0">>;
clean_constraint([VC]) ->
    clean_constraint(VC);
clean_constraint(VC) when is_binary(VC) ->
    case chef_object:parse_constraint(VC) of
        error ->
            %% what now?
            error({invalid_constraint, VC});
        {_, _} ->
            VC
    end.

soft_validate(Type, Fun, Org, OldId, Data, Name) ->
    try
        {ok, NewData} = Fun(chef_json:encode(Data)),
        NewData
    catch
        Error:Why ->
            lager:warning(?LOG_META(Org), "~s FAILED_TO_VALIDATE ~p ~p ~p ~p",
                       [Type, Error, Why, OldId, Name]),
            Data
    end.

%%
%% Utility routines
%%

%% This needs to look up the mixlib auth doc, find the user side id and the requester id,
%% map the user side id via opscode_account to the auth side id and return a tuple
get_authz_info(_Org, _Type, unset_name, _Id) ->
    not_found;
get_authz_info(Org, Type, Name, Id) ->
    {UserId, RequesterId} = get_user_side_auth_id(Org, Type, Name, Id),
    case user_to_auth(Org, UserId) of
        {ok, AuthId} ->
            case RequesterId of
                clone ->
                    {AuthId, AuthId};
                _  ->
                    {AuthId,  RequesterId}
            end;
        {fail, FailType} ->
            Props = [{error_type, FailType} | ?LOG_META(Org)],
            lager:warning(Props, "SKIPPING ~s ~s (~s) missing authz data",
                          [Type, Name, Id]),
            not_found
    end.

%% This needs to look up the mixlib auth doc, find the user side id and the requester id,
%% map the user side id via opscode_account to the auth side id and return a tuple
get_user_side_auth_id(#org_info{}, client, _Name, Id) ->
    %% Clients are special; they are their own mixlib auth docs
    {Id, clone};
get_user_side_auth_id(#org_info{auth_ets=Auth}, cookbook_version, Name, _Id) ->
    case ets:lookup(Auth, {cookbook, Name}) of
        [{_, {UserId, Data}}] ->
            ets:lookup(Auth, {cookbook, Name}),
            Requester = ej:get({"requester_id"}, Data),
            {UserId, Requester};
        [] ->
            {not_found, not_found}
    end;
get_user_side_auth_id(#org_info{auth_ets=Auth}, databag_item, Name, _Id) ->
    get_user_side_auth_id_generic(Auth, databag, Name);
get_user_side_auth_id(#org_info{auth_ets=Auth}, databag, Name, _Id) ->
    get_user_side_auth_id_generic(Auth, databag, Name);
get_user_side_auth_id(#org_info{auth_ets=Auth}, role, Name, _Id) ->
    get_user_side_auth_id_generic(Auth, role, Name);
get_user_side_auth_id(#org_info{auth_ets=Auth}, environment = Type, Name, _Id) ->
    get_user_side_auth_id_generic(Auth, Type, Name);
get_user_side_auth_id(Org, Type, Name, Id) ->
    lager:error(?LOG_META(Org), "Can't process for type ~s, ~s ~s", [Type, Name, Id]),
    {not_found, not_found}.

get_user_side_auth_id_generic(Auth, Type, Name) ->
    case ets:lookup(Auth, {Type, Name}) of
        [{_, {UserId, Data}}] ->
            Requester = ej:get({"requester_id"}, Data),
            {UserId, Requester};
        [] ->
            {not_found, not_found}
    end.

user_to_auth(_, not_found) ->
    {fail, user_side_authz_not_found};
user_to_auth(#org_info{account_info=Acct}=Org, UserId) ->
    moser_acct_processor:user_to_auth(Acct, UserId, ?LOG_META(Org)).

sqerl_delete_helper(Table, Where) ->
    case sqerl:adhoc_delete(Table, Where) of
        {ok, X} ->
            {ok, Table, X};
        {error, Error} ->
            lager:error("Cleanup error ~p ~p ~p", [Error, Table, Where]),
            {error, Table, Error}
    end.

delete_table_for_org("cookbook_versions", OrgId) ->
    Stmt = iolist_to_binary(["delete from cookbook_versions using cookbooks ",
                             "where cookbook_id = cookbooks.id and cookbooks.org_id = '",
                             OrgId, "';"]),
    case sqerl:execute(Stmt) of
        {ok, X} ->
            {ok, "cookbook_versions", X};
        {error, Error} ->
            lager:error("Cleanup error ~p ~p ~p",
                        [Error, "cookbook_versions", Stmt]),
            {error, "cookbook_versions", Error}
    end;
delete_table_for_org(Table,OrgId) ->
    sqerl_delete_helper(Table, {"org_id", equals, OrgId}).

cleanup_organization(OrgName) ->
    cleanup_orgid(moser_utils:orgname_to_guid(OrgName)).

cleanup_orgid(OrgId) ->
    [ delete_table_for_org(Table, OrgId) || Table <- ?SQL_TABLES ].

cleanup_all() ->
    [ sqerl_delete_helper(Q, all) || Q <- ?SQL_TABLES ].
