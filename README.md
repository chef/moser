# moser #

erchef couch migration tool

This tool migrates data from the couchdb data schema used by various
versions of chef to the sql schema used by chef.

This is named in honor of Leo Moser who first formulated the moving
sofa problem. [http://en.wikipedia.org/wiki/Moving_sofa_problem]
Hopefully moving this couch will be easier.


## Author(s) ##

* Mark Anderson <mark@opscode.com>

## Copyright ##

Copyright (c) 2012 Opscode, Inc.  All rights reserved.

## Usage ##

Build in dev-vm with
```
sudo make relclean rel
```

Verify that you are using the right database and service in rel/moser/etc/app.confing
* Specifically, pgsql service, port and password,
* db_name should be opscode_chef_test for early testing

Run in console with
```
sudo rel/moser/bin/moser console
```

First things first, load the opscode_account database into DETS tables:
```
moser_acct_processor:process_account_file().
```

The next step is to process the organization database into an ETS table:
```
f(Db), Db = moser_chef_processor:process_organization("ponyville").
```

Will return an org_info record for that :
```
{org_info,<<"ponyville">>,
          "3f0cbfe0b0c0474d9ac86a8fd51d6a30",
          "/srv/piab/mounts/moser/chef_3f0cbfe0b0c0474d9ac86a8fd51d6a30.couch",
          65569,69666,
          {user_to_authz,orgname_to_guid,authz_to_user,account_db},
          {1358,878871,354636}}
```

To do the db insert:
```
moser_chef_converter:insert(Db).
```

### Test sweep ###

```
f(CL), f(CLS), f(CO), f(CO2), f(T), f(R), f(X).
CL = moser_converter:get_chef_list(), length(CL).
CLS = lists:sublist(CL, 20, 10).
CO = moser_converter:file_list_to_orginfo(CLS), length(CO).
CO2 = moser_converter:filter_out_precreated_orgs(CO), length(CO2).
{T, R} = timer:tc(fun() -> moser_converter:process_insert_orgs(CO2) end).
X = lists:zip(R,CO2)
IsFail = fun({{ok, _},_}) -> false; (_) -> true end.
Fails = [ PP || PP <- Out, IsFail(PP)].
```

### Full sweep migration ###

```
%% should only have to do this once?
%% moser_acct_processor:process_account_file().
CL = moser_converter:get_chef_list(), length(CL).
CO = moser_converter:file_list_to_orginfo(CL), length(CO).
CO2 = moser_converter:filter_out_precreated_orgs(CO), length(CO2).
{T, R} = timer:tc(fun() -> moser_converter:process_insert_orgs(CO2) end).
> X = lists:zip(R,CO2)
> IsFail = fun({{ok, _},_}) -> false; (_) -> true end.
> Fails = [ PP || PP <- Out, IsFail(PP)].
> file:write_file("/home/mark/failures",io_lib:fwrite("~p.\n",[Fails])).

```

```
sudo su postgres
dropdb opscode_chef_test 
createdb -O opscode_chef opscode_chef_test
psql -h localhost -U opscode_chef opscode_chef_test < /home/seth/opscode_chef_schema.sql
```
