# Postgres Plugin

This plugin makes a backup of each database found in Postgres in a single file.
For restore select the needed database file, found in `/_Postgresbackups_` in the catalog. I literally took the plugin-mysql and ran sed across it to make this plugin. Then made some minor tweaks. 

##ERRORS
do not try to run estimate from the bconsole as it will cause this plugin to hang dumping the databases. You will need to restart the bareos-fd client to get it back. I'm not sure how to fix this yet. Working onit. 

## Prerequisites
The `pg_dump` and `psql` command must be installed and user root will sudo up to postgres 
to execute these commands. See below, if you want to use another user / password or want to
 restrict database access for the backup user.

You need the packages bareos-filedaemon-python-plugin installed on your client.

## Configuration

### Activate your plugin directory in the fd resource conf on the client
```
FileDaemon {                          
  Name = client-fd
  ...
  Plugin Directory = /usr/lib64/bareos/plugins
}
```

### Include the Plugin in the fileset definition on the director
```
FileSet {
    Name = "client-data"
       Include  {
                Options {
                        compression=GZIP
                        signature = MD5
                }
                Plugin = "python:module_path=/usr/lib/bareos/plugins:module_name=bareos-fd-postgresql"
        }
}
```

#### Options ####

##### Databases #####
By default all found databases are backed up, if the paramater 'db' is unset. You can have the plugin only certain databases saved, if you add
the parameter db=db1,db2, with a comma separated list of datbases. Also you can exclude specific database from backup by 'ignore_db' option with a comma separated list ignored databases.
Example plugin string:
```
               Plugin = "python:module_path=/usr/lib/bareos/plugins:module_name=bareos-fd-postgresql:db=test,postgres"
```
This will backup only the databases with name 'test' and 'Postgres'.

##### Postgresdump options #####

By default the opition string
```
--format=p --blobs --clean
```
is used.

You may overwrite the whole option string with parameter dumpoptions or just supress the latter two, which are needed to include drop and
create database statements into the dump. With drop_and_recreate set to 'false', these options are skipped.

##### Database access /  user and password  #####

By default the root user (the user, which runs the Bareos filedaemon) is used to connect to the database. We recommend that you set
a password for the user and use the defaultsfile option to point to another client configuration file.
You can however set a user and / or password as plugin options:
```
Postgresuser=username:Postgrespassword=secret
```

##### dumpbinary #####

Command (with or without full path) to create the dumps. Default: pg_dump
