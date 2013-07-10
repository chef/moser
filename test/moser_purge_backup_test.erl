-module(moser_purge_backup_test).

-include_lib("eunit/include/eunit.hrl").

-define(BACKUP_CHEF_NAME, "chef_backup").
-define(TEST_TERMS, 
  [
    atom,
    {double, tuple},
    "string",
    <<"binary">>,
    [atom, {double, tuple}, "string", <<"binary">>],
    {atom, {double, tuple}, "string", <<"binary">>}
  ]).

-compile([export_all]).

all_test_() ->
  {foreach,
    fun() ->
        moser_purge_backup:open_backups(?BACKUP_CHEF_NAME)
    end,
    fun(_) ->
				moser_purge_backup:delete_backups(?BACKUP_CHEF_NAME)
    end,
   [{with, [T]} || T <- [
      fun backup_restore_twice/1
    ]]
  }.


backup_restore_twice({ChefID, AuthID}) ->
  ok = moser_purge_backup:backup(ChefID, [?TEST_TERMS]),
  ok = moser_purge_backup:backup(ChefID, [?TEST_TERMS]),
  ok = moser_purge_backup:backup(AuthID, [?TEST_TERMS]),
  ok = moser_purge_backup:backup(AuthID, [?TEST_TERMS]),
  {ChefRestoredTerms, AuthRestoredTerms} = moser_purge_backup:read_backups(?BACKUP_CHEF_NAME),
  ?assertEqual([?TEST_TERMS , ?TEST_TERMS], ChefRestoredTerms),
  ?assertEqual([?TEST_TERMS , ?TEST_TERMS], AuthRestoredTerms).

