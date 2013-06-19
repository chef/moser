-module(moser_chef_couch_removal).

-export([
    load_org_to_ets/1,
    delete_org/1,
    delete_org_docs/2
  ]).

-include("moser.hrl").
-include_lib("stdlib/include/qlc.hrl").

load_org_to_ets(OrgName) ->
  OrgInfo = #org_info{db_name = DbName} = moser_acct_processor:expand_org_info(#org_info{ org_name = OrgName}),
  case filelib:is_file(DbName) of
    false ->
        throw({no_such_file, DbName});
    true ->
        ok
  end,

  CData = ets:new(chef_removal_data, [set,public]),
  AData = ets:new(auth_removal_data, [set,public]),

  Org = OrgInfo#org_info{ chef_ets = CData,
                          auth_ets = AData,
                          start_time = os:timestamp()},
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
  Db = make_db_descriptor(<<"chef_", (Org#org_info.org_id)/binary>>),
  AuthDb = make_db_descriptor(<<"opscode_account">>),
  ok = delete_org_docs(Db, ets:table(Org#org_info.chef_ets)),
  ok = delete_auth_docs(AuthDb, ets:table(Org#org_info.auth_ets)),
  couchbeam:compact(Db).

delete_auth_docs(Db, Table) ->
  Query = qlc:q([
      [{DocId, [Rev]} || {_,_,Rev, DocId} <- dets:lookup(user_to_authz,UserId)]
      || {{_, UserId, _}, _, _} <- Table]),
  delete_from_cursor(Db, Query).

delete_org_docs(Db, Table) ->
  Query = qlc:q([{Key,[RevId]} || {{_Type,Key,_Json},RevId,_Org} <- Table]),
  delete_from_cursor(Db, Query).

delete_from_cursor(Db, Query) ->
  Cursor = qlc:cursor(Query),
  do_delete_docs(Db, Cursor),
  qlc:delete_cursor(Cursor).

do_delete_docs(Db, Cursor) ->
  case qlc:next_answers(Cursor, envy:get(moser, purge_chunksize, 1000, integer)) of
    [] ->
      ok;
    [[]] ->
      ok;
    DocsUnflattened ->
      Docs = lists:flatten(DocsUnflattened),
      {_, Server, _, _} = Db,
      Url = couchbeam:make_url(Server, [couchbeam:db_url(Db), "/", "_purge"],[]),
      Headers = [{"Content-Type", "application/json"}],
      EncodedDocs = jiffy:encode({Docs}),
      {ok, _, _, _} = couchbeam:db_request(post, Url, ["200"], [], Headers, EncodedDocs),
      timer:sleep(envy:get(moser, purge_throttle, 10, integer)),
      do_delete_docs(Db, Cursor)
  end.


make_db_descriptor(DbName) ->
  CouchHost = envy:get(chef_db, couchdb_host, string),
  CouchPort = envy:get(chef_db, couchdb_port, integer),
  {db, {server, CouchHost, CouchPort, [],[]}, DbName, []}.
