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

-include("moser.hrl").

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

insert(#org_info{org_name = Name, org_id = Guid} = Org) ->
    try
        {Time0, Totals0} = insert_checksums(Org, dict:new()),
        {Time1, Totals1} = insert_databags(Org, Totals0),
        {Time2, _} = insert_objects(Org, Totals1),
        TotalTime = Time0 + Time1 + Time2,
        io:format("Total Database ~s (org ~s) insertions took ~f seconds~n", [Name, Guid, moser_utils:us_to_secs(TotalTime)]),
        {ok, TotalTime}
    catch
        error:E ->
            ?debugFmt("~p~n~p~n", [E,erlang:get_stacktrace()]),
            {error, E}
    end.

%%
%% Checksums need to be inserted before other things
%%
insert_checksums(#org_info{org_name = Name, org_id = Guid, chef_ets = Chef} = Org, Totals) ->
    {Time, Totals1} = timer:tc(
                       ets, foldl, [fun(Item,Acc) ->
                                            insert_checksums(Org, Item, Acc)
                                    end,
                                    Totals, Chef] ),
%%    io:format("Insert Checksums Stats: ~p~n", [lists:sort(dict:to_list(Totals1))]),
    io:format("Database ~s (org ~s) checksum insertions took ~f seconds~n", [Name, Guid, moser_utils:us_to_secs(Time)]),
    {Time, Totals1}.


insert_checksums(Org, {{checksum = Type, _Name}, Data}, Acc) ->
    %%    ?debugVal({Type, Name}),
    %%    ?debugFmt("~p~n",[Data]),
    OrgId = moser_utils:get_org_id(Org),
    %%    ?debugVal(OrgId),
    Checksum = ej:get({"checksum"}, Data),
    case sqerl:statement(insert_checksum, [OrgId, Checksum], count) of
        {ok, 1} ->
            ok;
        Error -> %% TODO check errors better here
            Error
    end,
    dict:update_counter(Type, 1, Acc);
insert_checksums(_Org, {{_Type, _Id}, _Data} = _Item, Acc) ->
%%    RType = list_to_atom("SKIP_CK_" ++ atom_to_list(_Type)),
%%    dict:update_counter(RType, 1, Acc);
    Acc;
insert_checksums(_Org, {orgname,_}, Acc) ->
    Acc;
insert_checksums(_Org, Item, Acc) ->
    ?debugVal(Item),
    Acc.

%%
%% Databags need to be inserted before other things
%%
insert_databags(#org_info{org_name = Name, org_id = Guid, chef_ets = Chef} = Org, Totals) ->
    {Time, Totals1} = timer:tc(
                       ets, foldl, [fun(Item,Acc) ->
                                            insert_databag(Org, Item, Acc)
                                    end,
                                    Totals, Chef] ),
%%    io:format("Insert Databags Stats: ~p~n", [lists:sort(dict:to_list(Totals1))]),
    io:format("Database ~s (org ~s) databag insertions took ~f seconds~n", [Name, Guid, moser_utils:us_to_secs(Time)]),
    {Time, Totals1}.

insert_databag(Org, {{databag, Id}, Data} = Object, Acc) ->
    Name = name_for_object(Object),
    case get_authz_info(Org, databag, Name, Id) of
        {AuthzId, RequesterId} ->
            Name = ej:get({<<"name">>}, Data),
            DataBag = #chef_data_bag{
                         id = moser_utils:fix_chef_id(Id),
                         authz_id = AuthzId,
                         org_id = iolist_to_binary(Org#org_info.org_id),
                         name = Name
                        },
            ObjWithDate = chef_object:set_created(DataBag, RequesterId),
            {ok, 1} = chef_sql:create_data_bag(ObjWithDate),
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
    ?debugVal(Item),
    Acc.


%%
%% Insert remaining objects
%%
insert_objects(#org_info{org_name = Name, org_id = Guid, chef_ets = Chef} = Org, Totals) ->
    {Time, Totals1} = timer:tc(
                       ets, foldl, [fun(Item,Acc) ->
                                            try
                                                insert_one(Org, Item, Acc)
                                            catch
                                                Error:Why ->
                                                    lager:error("~p (~p) unable to in insert item {~p, ~p, ~p}",
                                                                [Name, Guid, Item, Error, Why]),
                                                    Acc
                                            end
                                    end,
                                    Totals, Chef] ),
    io:format("Insert Objects Stats: ~p~n", [lists:sort(dict:to_list(Totals1))]),
    io:format("Database ~s (org ~s) all others insertions took ~f seconds~n", [Name, Guid, moser_utils:us_to_secs(Time)]),
    {Time, Totals1}.



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
    ?debugVal(Item),
    Acc.

%% client
insert_one(Org, {{client, Name}, {Id, Data}}, AuthzId, RequesterId, Acc) ->
    {PubKey, PubKeyVersion} = extract_client_key_info(Data),
    IsValidator = is_validator(Org#org_info.org_name, Data),
    IsAdmin = false, %% This is a OSC feature, false everywhere else
    Client = #chef_client{
      id = moser_utils:fix_chef_id(Id),
      authz_id = AuthzId,
      org_id = iolist_to_binary(Org#org_info.org_id),
      name = Name,
      validator = IsValidator,
      admin = IsAdmin,
      public_key = PubKey,
      pubkey_version = PubKeyVersion
     },
    ObjWithDate = chef_object:set_created(Client, RequesterId),
    try
        {ok, 1} = chef_sql:create_client(ObjWithDate)
    catch
        error:E ->
            ?debugFmt("~p~n",[Data]),
            ?debugVal(E), ?debugVal(erlang:get_stacktrace())
    end,
    dict:update_counter(client, 1, Acc);
%% Data bag item
insert_one(Org, {{databag_item = Type, Id}, Data}, _AuthzId, RequesterId, Acc) ->
    %%    ?debugVal({Type, Id}),
    %%    ?debugFmt("~p~n",[Data]),
    RawData = ej:get({<<"raw_data">>}, Data),
    DataBagName = ej:get({<<"data_bag">>}, Data),
    ItemName = ej:get({<<"raw_data">>,<<"id">>}, Data),
    %% FIXME: shouldn't this be gzipp'ed
    SerializedObject = jiffy:encode(RawData),
    DataBagItem = #chef_data_bag_item{
      id = moser_utils:fix_chef_id(Id),
      org_id = iolist_to_binary(Org#org_info.org_id),
      data_bag_name = DataBagName,
      item_name = ItemName,
      serialized_object = SerializedObject
     },
    ObjWithDate = chef_object:set_created(DataBagItem, RequesterId),
    case chef_sql:create_data_bag_item(ObjWithDate) of
        {ok, 1} ->
            ok;
        {foreign_key, Msg} ->
            ?debugFmt("Error inserting ~s, ~s ~n~p~n", [Type, Msg, ObjWithDate]);
        {conflict, Msg} ->
            ?debugFmt("Error inserting ~s, ~s ~n~p~n", [Type, Msg, ObjWithDate])
    end,
    dict:update_counter(Type, 1, Acc);
%% Role
insert_one(Org, {{role = Type, Id}, Data}, AuthzId, RequesterId, Acc) ->
    Name = ej:get({<<"name">>}, Data),
    SerializedObject = jiffy:encode(Data),
    Role = #chef_role{
      id = moser_utils:fix_chef_id(Id),
      authz_id = AuthzId,
      org_id = moser_utils:get_org_id(Org),
      name = Name,
      serialized_object = SerializedObject
     },
    ObjWithDate = chef_object:set_created(Role, RequesterId),
    {ok, 1} = chef_sql:create_role(ObjWithDate),
    dict:update_counter(Type, 1, Acc);
%% Environments
insert_one(Org, {{environment = Type, Id}, Data}, AuthzId, RequesterId, Acc) ->
    Name = ej:get({<<"name">>}, Data),
    SerializedObject = jiffy:encode(Data),
    Role = #chef_environment{
      id = moser_utils:fix_chef_id(Id),
      authz_id = AuthzId,
      org_id = moser_utils:get_org_id(Org),
      name = Name,
      serialized_object = SerializedObject
     },
    ObjWithDate = chef_object:set_created(Role, RequesterId),
    {ok, 1} = chef_sql:create_environment(ObjWithDate),
    dict:update_counter(Type, 1, Acc);
%% Cookbook versions: This is so horridly wrong I'm ashamed, but it probably represents the IOP count properly
insert_one(Org, {{cookbook_version = Type, Id}, Data}, AuthzId, RequesterId, Acc) ->
%    ?debugVal({Type, Id}),
%    ?debugFmt("~p~n",[moser_utils:list_ej_keys(Data)]),
    _Checksums = extract_all_checksums(?SEGMENTS, Data),
%    ?debugFmt("~p~n", [lists:usort(Checksums)]),
    BaseRecord = chef_object:new_record(chef_cookbook_version, moser_utils:get_org_id(Org), AuthzId, Data),
    CookbookVersion = BaseRecord#chef_cookbook_version{id = moser_utils:fix_chef_id(Id)},
    ObjWithDate = chef_object:set_created(CookbookVersion, RequesterId),
%    ?debugFmt("~p~n",[ObjWithDate]),
    {ok, 1} = chef_sql:create_cookbook_version(ObjWithDate),
    dict:update_counter(Type, 1, Acc);
%% Old style cookbooks
%% These use the "Cookbook" chef_type. As best we can tell, this isn't used anywhere in the OHC code.
%% May want a final check with Adam and CB to make sure there isn't some secret stuff, but it appears
%% we can ignore them for now.
insert_one(_Org, {{cookbook_old = Type, _Id}, _Data}, _AuthzId, _RequesterId, Acc) ->
    %%?debugVal({_Type, _Id}),
    %%?debugFmt("~p~n",[_Data]),
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
insert_one(_Org, Item, _AuthzId, _RequesterId, Acc) ->
    ?debugVal(Item),
    Acc.

is_validator(OrgName, Data) ->
    %% TODO: the org record in opscode_account specifies the name of the validator; we should modify to use that
    Client = ej:get({"clientname"}, Data),
    {ok, RE} = re:compile("^(?<Org>.*)-validator$"),  %% Figure out how to do this only once
    case re:run(Client, RE, [{capture, ['Org'], binary}]) of
        {match, [OrgName]} ->
            true;
        _ ->
            false
    end.

extract_client_key_info(Data) ->
    case ej:get({"certificate"}, Data) of
        undefined ->
            case ej:get({"public_key"}, Data) of
                undefined ->
                    ?debugFmt("~p~n",[Data]),
                    %% figure out something better
                    {undefined, undefined};
                PubKey ->
                    {PubKey, ?KEY_VERSION}
            end;
        PubKey ->
            PubKeyVersion =  ?CERT_VERSION,
            {PubKey, PubKeyVersion}
    end.


%%
%% Utility routines
%%
extract_all_checksums(Sections, Data) ->
    lists:flatten([ extract_checksums(ej:get({Section}, Data)) || Section <- Sections ]).

extract_checksums(undefined) ->
    [];
extract_checksums(SegmentData) ->
    [ej:get({"checksum"}, Item) || Item <- SegmentData].


%% This needs to look up the mixlib auth doc, find the user side id and the requester id,
%% map the user side id via opscode_account to the auth side id and return a tuple
get_authz_info(_Org, _Type, unset_name, _Id) ->
    %% TODO: should this be not_found?
    {unset, unset};
get_authz_info(Org, Type, Name, Id) ->
    {UserId, RequesterId} = get_user_side_auth_id(Org, Type, Name, Id),
    AuthId = case user_to_auth(Org, UserId) of
                 {ok, A} -> A;
                 {fail, _} ->
                     Msg = iolist_to_binary(io_lib:format("~s No authz id found for ~s ~s ~s",
                                                          [Org#org_info.org_name, Type, Name, Id])),
                     ?debugFmt("~s", [Msg]),
                     not_found
             end,
    case RequesterId of
        clone -> {AuthId, AuthId};
        _  -> {AuthId,  RequesterId}
    end.

%%
%% This needs to look up the mixlib auth doc, find the user side id and the requester id,
%% map the user side id via opscode_account to the auth side id and return a tuple
get_user_side_auth_id(#org_info{}, client, _Name, Id) ->
    %% Clients are special; they are their own mixlib auth docs
    {Id, clone};
get_user_side_auth_id(#org_info{auth_ets=Auth}, cookbook_version, Name, _Id) ->
    [{_, {UserId, Data}}] = ets:lookup(Auth, {cookbook, Name}),
    Requester = ej:get({"requester_id"}, Data),
    {UserId, Requester};
get_user_side_auth_id(#org_info{auth_ets=Auth}, databag_item, Name, _Id) ->
    get_user_side_auth_id_generic(Auth, databag, Name);
get_user_side_auth_id(#org_info{auth_ets=Auth}, databag, Name, _Id) ->
    get_user_side_auth_id_generic(Auth, databag, Name);
get_user_side_auth_id(#org_info{auth_ets=Auth}, role, Name, _Id) ->
    get_user_side_auth_id_generic(Auth, role, Name);
get_user_side_auth_id(#org_info{auth_ets=Auth}, environment = Type, Name, _Id) ->
    get_user_side_auth_id_generic(Auth, Type, Name);
get_user_side_auth_id(_Org, Type, Name, Id) ->
    ?debugFmt("Can't process for type ~s, ~s ~s", [Type, Name, Id]),
    {bad_id, <<"BadId">>}.

get_user_side_auth_id_generic(Auth, Type, Name) ->
    case ets:lookup(Auth, {Type, Name}) of
        [{_, {UserId, Data}}] ->
            Requester = ej:get({"requester_id"}, Data),
            {UserId, Requester};
        [] ->
            %% TODO: Fix this to hard fail on error,
            ?debugFmt("Can't find auth info for ~s, ~s~n", [Type, Name]),
            {<<"DEADBEEF">>, <<"DEADD0G">>}
    end.

user_to_auth(#org_info{account_info=Acct}, UserId) ->
    moser_acct_processor:user_to_auth(Acct, UserId).

sqerl_delete_helper(Table, Where) ->
    case sqerl:adhoc_delete(Table, Where) of
        {ok, X} ->
            {ok, Table, X};
        {error, Error} ->
            ?debugFmt("Cleanup error ~p ~p ~p", [Error, Table, Where]),
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
            ?debugFmt("Cleanup error ~p ~p ~p", [Error, "cookbook_versions", Stmt]),
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
