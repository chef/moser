-module(moser_chef_couch_removal).

-export([
        load_org_to_ets/1,
        delete_org/1,
        make_auth_query/1,
        make_chef_query/1,
        load_org_record/1,
        make_chef_db_descriptor/1,
        make_auth_db_descriptor/0,
        validate_restored_org_against_couch/1,
        reset_purged_org/1
    ]).

-include("moser.hrl").
-include_lib("stdlib/include/qlc.hrl").

-define(COUCH_REST_TYPE, {<<"couchrest-type">>, <<"Mixlib::Authorization::AuthJoin">>}).
-define(USER_TYPE(U2AId), {<<"user_object_id">>, U2AId}).
-define(AUTH_TYPE(U2AId), {<<"auth_object_id">>, A2UId}).

load_org_to_ets(OrgName) ->
    OrgInfo = #org_info{db_name = DbName} = load_org_record(OrgName),
    case filelib:is_file(DbName) of
        false ->
            throw({no_such_file, DbName});
        true ->
            ok
    end,

    CData = ets:new(chef_removal_data, [set,public]),
    AData = ets:new(auth_removal_data, [set,public]),

    Org = OrgInfo#org_info{ chef_ets = CData,
        auth_ets = AData},
    IterFn = fun(Key, RevId, Body, AccIn) ->
            process_couch_item(Org, Key, RevId, Body),
            AccIn
    end,
    decouch_reader:open_process_all(DbName, IterFn),
    {ok, Org}.

process_couch_item(Org, Key, RevId, Body) ->
    Type = moser_chef_processor:extract_type(Key, Body),
    InsertionTarget = filter_type(Type),
    insert(InsertionTarget, Type, Key, RevId, Body, Org).

filter_type({auth_simple, container}) ->
    no_op;
filter_type(auth_org) ->
    no_op;
filter_type(auth_user) ->
    no_op;
filter_type({auth, group}) ->
    no_op;
filter_type({auth_simple, _}) ->
    auth_ets;
filter_type(auth_join) ->
    auth_ets;
filter_type(undefined) ->
    no_op;
filter_type({auth, client}) ->
    auth_ets;
filter_type({_, node}) ->
    no_op;
filter_type(design_doc) ->
    no_op;
filter_type(_) ->
    chef_ets.

insert(no_op, _Type, _Key, _RevId, _Body, _Org) ->
    no_op;

insert(chef_ets, Type, Key, RevId, Body, Org) ->
    ets:insert(Org#org_info.chef_ets, {{Type, Key, Body}, RevId, Org});
insert(auth_ets, Type, UserIdOrAuthId, RevId, Body, Org) ->
    ets:insert(Org#org_info.chef_ets, {{Type, UserIdOrAuthId, Body}, RevId, Org}),
    ets:insert(Org#org_info.auth_ets, {{Type, UserIdOrAuthId, Body}, RevId, Org}).

delete_org(OrgName) when is_list(OrgName) ->
    delete_org(list_to_binary(OrgName));
delete_org(OrgName) when is_binary(OrgName) ->
    {ok, Org} = load_org_to_ets(OrgName),
    delete_org(Org),
    ets:delete(Org#org_info.chef_ets),
    ets:delete(Org#org_info.auth_ets),
    ok;
delete_org(Org) ->
    {ChefBackupID, AuthBackupID} = moser_purge_backup:open_backups(Org#org_info.org_id),
    ChefDb = make_chef_db_descriptor(Org),
    ok = delete_from_cursor(ChefDb, make_chef_query(Org), ChefBackupID),
    ok = case envy:get(moser, purge_auth, false, boolean) of
      true ->
        AuthDb = make_auth_db_descriptor(),
        delete_from_cursor(AuthDb, make_auth_query(Org), AuthBackupID);
      false ->
        ok
    end,
    {ok, ok} = moser_purge_backup:close_backups(ChefBackupID, AuthBackupID),
    couchbeam:compact(ChefDb).

delete_from_cursor(Db, Query, BackupIoDevice) ->
    Cursor = qlc:cursor(Query),
    do_delete_docs(Db, Cursor, BackupIoDevice),
    qlc:delete_cursor(Cursor).

do_delete_docs(Db, Cursor, BackupIoDevice) ->
    case lists:flatten(qlc:next_answers(Cursor, envy:get(moser, purge_chunksize, 1000, integer))) of
        [] ->
            ok;
        DocsRaw ->
            Docs = transform_to_purge(DocsRaw),
            BackupReadyDocs = transform_to_backup(DocsRaw),
            Url = make_url(Db, "_purge"),
            Headers = [{"Content-Type", "application/json"}],
            EncodedDocs = jiffy:encode({Docs}),
            {ok, _, _, _} = couchbeam:db_request(post, Url, ["200"], [], Headers, EncodedDocs),
            ok = moser_purge_backup:backup(BackupIoDevice, BackupReadyDocs),
            timer:sleep(envy:get(moser, purge_throttle, 10, integer)),
            do_delete_docs(Db, Cursor, BackupIoDevice)
    end.


make_db_descriptor(DbName) ->
    CouchHost = envy:get(chef_db, couchdb_host, string),
    CouchPort = envy:get(chef_db, couchdb_port, integer),
    {db, {server, CouchHost, CouchPort, [],[]}, DbName, []}.


transform_to_purge([{_, _, _, _} | _] = Terms) ->
    [{DocId, [Rev]} || {_,_,Rev, DocId} <- Terms];

transform_to_purge([{{_,_,_}, _,_} | _Rest] = Terms) ->
    [{DocId,[Rev]} || {{_,DocId,_},Rev,_} <- Terms].

transform_to_backup([{_, _, _, _} | _] = Terms) ->
    [{[{<<"_id">>, DocId}, ?USER_TYPE(U2AId), ?AUTH_TYPE(A2UId), ?COUCH_REST_TYPE ]} || {U2AId, A2UId, _Rev, DocId } <- Terms];

transform_to_backup([{{_,_,_}, _,_} | _] = Terms) ->
    [{[{<<"_id">>, DocId} | JsonProplist]} || {{_Type,DocId,{JsonProplist}}, _Rev,_Org} <- Terms].


make_auth_query(Org) ->
    qlc:q([
            [AuthRecord || AuthRecord <- dets:lookup(user_to_authz,UserId), AuthRecord =/= []]
            || {{_, UserId, _}, _, _} <- ets:table(Org#org_info.auth_ets)]).

make_chef_query(Org) ->
    qlc:q([ChefRecord || ChefRecord <- ets:table(Org#org_info.chef_ets)]).

load_org_record(OrgName) ->
    moser_acct_processor:expand_org_info(#org_info{ org_name = OrgName}).

make_chef_db_descriptor(Org) ->
    make_db_descriptor(<<"chef_", (Org#org_info.org_id)/binary>>).

make_auth_db_descriptor() ->
    make_db_descriptor(<<"opscode_account">>).

make_url(DbDescriptor, Endpoint) ->
    Server = extract_server_from_db_descriptor(DbDescriptor),
    couchbeam:make_url(Server, [couchbeam:db_url(DbDescriptor), "/", Endpoint],[]).

extract_server_from_db_descriptor(DbDescriptor) ->
    {_, Server, _, _} = DbDescriptor,
    Server.

validate_restored_org_against_couch(InputOrgName) when is_list(InputOrgName) ->
    validate_restored_org_against_couch(list_to_binary(InputOrgName));

validate_restored_org_against_couch(InputOrgName) when is_binary(InputOrgName) ->
    {ok, Org} = load_org_to_ets(InputOrgName),
    validate_restored_org_against_couch(Org);

validate_restored_org_against_couch(InputOrgInfo = #org_info{}) ->
    AuthDb = make_auth_db_descriptor(),
    ChefDb = make_chef_db_descriptor(InputOrgInfo),
    {ChefTerms, AuthTerms} = moser_purge_backup:read_backups(InputOrgInfo),
    true = verify_terms(ChefTerms, ChefDb),
    true = verify_terms(AuthTerms, AuthDb),
    true.


verify_terms(Terms, Db) ->
    lists:all(fun({[{<<"_id">>, DocId} | Rest]}) ->
                {ok, {[_Id, _Rev | Remainder]}} = couchbeam:open_doc(Db, DocId),
                case lists:sort(Rest) == lists:sort(Remainder) of
                    false ->
                        io:format("DocId ~p failed to validate~n CouchData:~p~n RestoreData ~p~n", [DocId, Remainder, Rest]),
                        false;
                    true ->
                        true
                end
        end,
        Terms).

reset_purged_org(OrgName) when is_list(OrgName) ->
    reset_purged_org(list_to_binary(OrgName));
reset_purged_org(OrgName) ->
    moser_state_tracker:reset_purged_orgs(),
    moser_state_tracker:ready_migration(OrgName),
    moser_state_tracker:migration_started(OrgName),
    ok = moser_state_tracker:migration_successful(OrgName).
