## OHC / Erchef Cookbook Dependency Validation ##

### Setup ###

* Start an OPC instance built from the `sd/test-dependency-resolution` branch
* Install mysql: `sudo apt-get install mysql-server`
* Download Production Data Snapshot
    * Snapshots located @ zmanda-rspreprod-i-d321f0a2.opscode.us:/backups
* Load Snapshot Into MySQL
    * `mysql -uroot "create database opscode_chef;"`
    * `mysql -uroot opscode_chef < backup.sql`
* Dump the MySQL database into PostgreSQL format
    * dump/restore scripts located [here](https://gist.github.com/sdelano/1ce71632b9e93818f9b1)

### Moser ###

Run the `moser` tool as described in the main project [README](https://github.com/opscode/moser/blob/master/README.md).

### Correctness Test ###

From the `correctness-test/scripts` directory, run `knife exec validate.rb -c PATH_TO_KNIFE_RB`

Since the installed branch of OPC has authz and user_in_org checks disabled, you only need a `knife.rb` configuration file that belongs to a user that is active in your OPC install. Those users include all of the "My Little Brony" users that are default in new dev-vm instances and all valid users in production (though you'll need their private key). Here is an example `knife.rb`, where the `applejack` dev-vm user is configured to read data from the `prezi` production org:

```ruby
current_dir = File.dirname(__FILE__)
log_level :info
log_location STDOUT
node_name "applejack"
client_key "#{current_dir}/applejack.pem"
chef_server_url "https://api.opscode.piab/organizations/prezi"
```

