%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92-*-
%% ex: ts=4 sw=4 et
%%
%% @author Marc Paradise <marc@opscode.com>
%% @copyright 2013, Opscode Inc
%%

-module(moser_user_hash_extractor).

-export([update_all_users/0,
         update_users/1,
         unconverted_users/0,
         unconverted_users/1,
         convert_user/1]).

-record(tiny_user, {
        'username',         %% capture for debugging purposes.
        'serialized_object' %% that thing we're going to tear apart and reassemble
       }).

update_users(Count) ->
    {ok, Users} = unconverted_users(Count),
    [convert_user(User) || User <- Users].

update_all_users() ->
    % We'll do this in two passes to avoid loading the entire users table (mostly the
    % serialized_objects) into memory at once.
    {ok, Users} = unconverted_users(),
    [convert_user(User) || User <- Users].

convert_user(Id) ->
    {ok, #tiny_user{username = Name, serialized_object = Object}} = user_data(Id),
    Json1 = chef_json:decode(Object),
    {Hash, Json2} = get_and_delete(<<"hashed_password">>, Json1),
    {Salt, Json3} = get_and_delete(<<"salt">>, Json2),
    Encoded = chef_json:encode(Json3),
    update_user_record(Name, Id, Hash, Salt, Encoded).

user_data(Id) ->
    case sqerl:execute(<<"SELECT username, serialized_object FROM users WHERE id = $1">>, [Id]) of
        {error, Error} ->
            lager:error("Failed to fetch user detail: ~p", [Error]),
            {error, Error};
        {ok, Rows } when is_list(Rows) ->
            XF = sqerl_transformers:rows_as_records(tiny_user, record_info(fields, tiny_user)),
            {ok, [Data]} = XF(Rows),
            {ok, Data}
    end.

get_and_delete(Key, Json) ->
    Value = ej:get({Key}, Json),
    Final = ej:delete({Key}, Json),
    {Value, Final}.

update_user_record(_Name, Id, Hash, Salt, Encoded) ->
    case sqerl:execute(user_update_sql(), [Id, Hash, Salt, Encoded]) of
        {ok, _Num} ->
            ok;
        {error, Error} ->
            lager:error("User updated failed: ~p", [Error]),
            {error, Error}
    end.

user_update_sql() ->
    <<"UPDATE users
          SET hash_type = 'SHA1', hashed_password = $2, salt = $3, serialized_object = $4
        WHERE id = $1">>.

unconverted_users(Max) ->
    fetch_field_values(id, <<"SELECT id FROM users WHERE hashed_password = '' ORDER BY created_at LIMIT $1">>, [Max]).

unconverted_users() ->
    fetch_field_values(id, <<"SELECT id FROM users WHERE hashed_password = '' ORDER BY created_at">>, []).

fetch_field_values(FieldName, SQL, Args) ->
    case sqerl:execute(SQL, Args) of
        {error, Error} ->
            lager:error("Failed to fetch user list: ~p", [Error]),
            {error, Error};
        {ok, []} ->
            {ok, []};
        {ok, Rows } when is_list(Rows) ->
            XF = sqerl_transformers:rows_as_scalars(FieldName),
            XF(Rows)
    end.
