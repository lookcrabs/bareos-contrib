# S3 Plugin

This plugin makes a backup of each bucket+key found in s3.
For restore select the needed database file, found in `/_bucket_/key` in the catalog. I literally took the plugin-mysql and bashed this into it. It's gross but I don't know how to do any better. 

## ERRORS
do not try to run estimate from the bconsole as it will cause this plugin to hang. You will need to restart the bareos-fd client to get it back. I'm not sure how to fix this yet. Working onit. 

## Prerequisites
  * The boto3 needs to be installed. `pip install boto3`.
  * a creds.json file needs to exist somewhere where bareos can access it. I threw mine in /etc/bareos/

## Configuration

Just take the creds.json file here and edit it for your needs. Here we specify all of the options:

   * access_key == your s3 access key
   * secret_key == your s3 secret key
   * host == your full url for your S3 endpoint. I set this up using Cleversafe and Ceph so for me it is https://cleversafe.internal.tld but for you it may be different
   * is_secure == verify your ssl certificates and use https? Most likely this is YES
   * signature == awsv4 or awsv2 signatures. `s3` or `s3v4` I believe
   * astyle == addressing style. I think the options are path or virtual. I used path for Ceph.
   * bucket_list == This is optional but should be a list of buckets you want to back up. If this isn't specified it tries everything it has read access to. Maybe not the best?
   * exclude_buckets is only read if you don't specify bucket_list. This will exclude buckets from the list_all

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
  Name = "S3BucketDump"
  Description = "Backup all buckets via boto3 and then stream to director"
  Include {
    Options {
      compression=GZIP
      signature = MD5
    }
    Plugin = "python:module_path=/usr/lib/bareos/plugins:module_name=bareos-fd-s3:configfile=/etc/bareos/creds.json"
  }
}
```

### test with a test job
```bconsole configure add client name="client_name" address="address_of_client"```
```bconsole configure add job name="client_name-job_name-test" client="client_name" jobdefs=S3BucketDump```
```run job=ss-psql-bareos-test-03-boto-backup-test yes```

That should create your initial full backup and test the job


Someone please save me from this mess and help me correct this.
I tried to comment as much as I could but if anything is messed up please let me know and I'll try and fix as best as I can.

