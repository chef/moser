-module(moser_purge_backup).

-export([
        backup/2,
        read_backup_file/1,
        read_backups/1,
        open_backups/1,
        close_backups/2,
        delete_backups/1,
        restore_org/1
    ]).

-include_lib("moser/include/moser.hrl").

backup(disabled, _) ->
    ok;
backup(IoDevice, Terms) ->
    BinaryTerms = convert_terms_to_binary(Terms),
    file:write(IoDevice, BinaryTerms).

read_backup_file(BackupFileName) ->
    {ok, Binary} = file:read_file(BackupFileName),
    convert_binary_to_terms(Binary).

delete_backups(OrgId) ->
    ok = file:delete(make_chef_backup_name(OrgId)),
    ok = file:delete(make_auth_backup_name(OrgId)),
    ok.

read_backups(#org_info{org_id = OrgId}) ->
    read_backups(OrgId);
read_backups(OrgId) ->
    {ok, ChefBinary} = file:read_file(make_chef_backup_name(OrgId)),
    {ok, AuthBinary} = file:read_file(make_auth_backup_name(OrgId)),
    {convert_binary_to_terms(ChefBinary), convert_binary_to_terms(AuthBinary)}.

open_backups(OrgId) ->
    case envy:get(moser, purge_backup, true, boolean) of
        true ->
            {ok, ChefIoDevice} = file:open(make_chef_backup_name(OrgId), [read, write]),
            {ok, AuthIoDevice} = file:open(make_auth_backup_name(OrgId), [read, write]),
            {ChefIoDevice, AuthIoDevice};
        false ->
            lager:info("Backups disabled for purging org ~p", [OrgId]),
            {disabled, disabled}
    end.

close_backups(disabled, disabled) ->
    {ok, ok};
close_backups(ChefIoDevice, AuthIoDevice) ->
    {file:close(ChefIoDevice), file:close(AuthIoDevice)}.

restore_org(Org) when is_binary(Org) ->
    restore_org(moser_chef_couch_removal:load_org_record(Org));
restore_org(Org = #org_info{org_id = OrgId}) ->
    {ChefTerms, AuthTerms} = read_backups(OrgId),
	restore(ChefTerms, length(ChefTerms), moser_chef_couch_removal:make_chef_db_descriptor(Org)),
	restore(AuthTerms, length(AuthTerms), moser_chef_couch_removal:make_auth_db_descriptor()).

restore([], _, _) ->
    ok;

restore(Terms, Length, DbDescriptor) ->
	SublistSize = envy:get(moser, purge_chunksize, 1000, integer),
	{TermsToRestore, RemainingTerms} = case Length > SublistSize of
		true ->
			lists:split(SublistSize, Terms);
		false ->
			lists:split(Length, Terms)
	end,
    restore_terms(TermsToRestore, DbDescriptor),
    restore(RemainingTerms, Length - SublistSize, DbDescriptor).

restore_terms(Terms, DbDescriptor) ->
    {ok, _} = couchbeam:save_docs(DbDescriptor, Terms).




make_chef_backup_name(OrgId) ->
    make_backup_name(OrgId , "_chef_backup.bin").

make_auth_backup_name(OrgId) ->
    make_backup_name(OrgId , "_auth_backup.bin").

make_backup_name(OrgId, Suffix) when is_binary(OrgId) ->
    make_backup_name(binary_to_list(OrgId), Suffix);

make_backup_name(OrgId, Suffix) ->
    Prefix = envy:get(moser, purge_backup_dir, ".", string),
    filename:join([Prefix , OrgId ++ Suffix]).

convert_terms_to_binary(Terms) ->
    lists:foldl(fun(Term, CurrentBinary) ->
                BinaryTerm = term_to_binary(Term),
                <<(size(BinaryTerm)):8/integer-unit:8, BinaryTerm/binary, CurrentBinary/binary>>
        end,
        <<>>,
        Terms
    ).

convert_binary_to_terms(Terms) ->
    convert_binary_to_terms(Terms, []).

convert_binary_to_terms(<<>>, Terms) ->
    Terms;

convert_binary_to_terms(<<Size:8/integer-unit:8, NextTerm:Size/binary, Rest/binary>>, Terms ) ->
    convert_binary_to_terms(Rest, [binary_to_term(NextTerm) | Terms]).

