#!/usr/bin/env python
import boto3
import botocore
from botocore.client import Config
import datetime
import time
import json
import os
#import shutil
import io
import hashlib
import sys

def getcreds(fpath):
    creds = json.load(open(fpath))
    return creds

def conngen(creds):
    s3 =  boto3.client('s3',
                  endpoint_url = creds['host'],
                  aws_access_key_id = creds['access_key'],
                  aws_secret_access_key = creds['secret_key'],
                  config = Config(signature_version = creds['signature'],
                                  s3={'addressing_style': creds['astyle']})
                 )
    return s3

def chunky_download(conn, bucketname, keyname):
    '''
    Stolen from here:
    https://gist.github.com/veselosky/9427faa38cee75cd8e27
    Can possibly be replaced with download_fileobj()
    https://boto3.readthedocs.io/en/latest/reference/services/s3.html#S3.Client.download_fileobj
    '''
    retr = conn.get_object(Bucket = bucketname, Key = keyname)
    bytestream = io.BytesIO(retr['Body'].read())
    return bytestream

def uploadfile(conn, bucketname, fname):
        #conn.upload_file(Filename = fname, Bucket = bucketname, Key = os.path.basename(fname))
        pass

def list_buckets(conn):
    for bucketname in [bucket['Name'] for bucket in conn.list_buckets()['Buckets']]:
        yield bucketname

def check_key_exists(conn, bucketname, keyname):
    try:
        conn.head_object(Bucket = bucketname, Key = keyname)
        return True
    except botocore.exceptions.ClientError:
        return False

def check_bucket_exists(conn, bucketname):
    try:
        conn.head_bucket(Bucket = bucketname)
        return True
    except botocore.exceptions.ClientError:
        return False

def list_objects(conn,bucket=None):
    paginator = conn.get_paginator('list_objects')
    if bucket and bucket is not None:
        op_perams = {'Bucket': bucket }
        page_iterator = paginator.paginate(**op_perams)
        ittor=0
        for page in page_iterator:
            s3keys = page['Contents']
#            print("{knum:<5s} {name:<120s} {owner:30s} {modified:50} {size:30}".format(
#                knum = "#",
#                name = "Name",
#                owner = "Owner",
#                modified = "Last Modified",
#                size = "Size"))
            for key in s3keys:
                yield key['Key']
#            for keyi in range(len(s3keys)):
#                key = s3keys[keyi]
                """{
                    u'LastModified': datetime.datetime(2017, 7, 27, 20, 14, 44, 190000, tzinfo=tzlocal()),
                    u'ETag': '"0912281624578d559522ae649b1bc8e9"',
                    u'StorageClass': 'STANDARD',
                    u'Key': 'fb/af827372-7426-4ecb-85de-f4fc2d9ea655/af827372-7426-4ecb-85de-f4fc2d9ea655.vcf.gz',
                    u'Owner': {u'DisplayName': 'FORTUNO',
                    u'ID': '73eb6c0f-c9c1-4a5e-b482-3577b0e1f787'},
                    u'Size': 961099
                    }"""
#                last_modified = str(key['LastModified'])
#                size = key['Size']
#                owner = key['Owner']['DisplayName']
#                etag = key['ETag']
#                key_name = key['Key']
                #print(s3keys[key])
#                print("{knum:<5d} {name:<120s} {owner:30s} {modified:50} {size:30d}".format(
#                    knum = ittor,
#                    name = key_name,
#                    owner = owner,
#                    modified = last_modified,
#                    size = size))
#                ittor+=1

def get_key_size(conn, bucketname, keyname):
    return conn.head_object(Bucket = bucketname, Key = keyname)['ContentLength']

def get_key_mtime(conn, bucketname, keyname):
    ktime = conn.head_object(Bucket = bucketname, Key = keyname)['LastModified']
    return int(ktime.strftime("%s")) * 1.0

def download_key(conn, bucketname, keyname, localpath=None):
    '''
    Just a wrapper around chunky_download to see if downloading files works
    '''
    if localpath and localpath is not None:
        pass
    else:
        localpath = os.getcwd()
        localpath += '/' + str(keyname.split('/')[-1])

    keysize = get_key_size(conn, bucketname, keyname)

    hash_md5 = hashlib.md5()
    with open(localpath, 'ab') as f:
        for chunk in chunky_download(conn, bucketname, keyname):
            hash_md5.update(chunk)
            f.write(chunk)
    key_md5 = hash_md5.hexdigest()
    localsize = os.path.getsize(localpath)
    print("+----------------------------------------------------------------+")
    print("|| {keyname} | {keysize} | {filesize} | {md5} ||".format(
            keyname = 'Keyname',
            keysize = 'Keysize',
            filesize = 'Filesize',
            md5 = 'MD5'))
    print("+----------------------------------------------------------------+")
    print("|| {keyname} | {keysize} | {filesize} | {md5} ||".format(
            keyname = keyname,
            keysize = keysize,
            filesize = localsize,
            md5 = key_md5))
    print("+----------------------------------------------------------------+")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='s3 uploader')
    parser.add_argument('-f','--fname', type=str, help="File path", required=False)
    parser.add_argument('-c','--creds', type=str, help="Credentials file path", required=True)
    parser.add_argument('-b', '--bucket', type=str, help="Specify bucket", required=False)
    parser.add_argument('-k', '--key', type=str, help="Specify key name", required=False)

    parser.add_argument('--download', action='store_true', help="Download Key locally", required=False)

    parser.add_argument('--localpath', type=str, help="path to and including local filename to download", required=False)
    parser.add_argument('--getkeysize', action='store_true', help="Get key Size", required=False)
    parser.add_argument('--listbuckets', action='store_true', help="List Buckets", required=False)
    parser.add_argument('--listobjects', action='store_true', help="List objects", required=False)
    args = parser.parse_args()

    creds = getcreds(args.creds)
    s3 = conngen(creds)

    if args.bucket:
        bname = args.bucket
    else:
        if 'bname' in creds:
            bname = creds['bname']
        else:
            bname = None

    if args.key:
        kname = args.key
    else:
        kname = None

    if args.listbuckets == True:
        for bucketname in list_buckets(s3):
            print(bucketname)

    if args.listobjects == True:
        if bname and bname is not None:
            for key in list_objects(s3, bname):
                print(key)
        else:
            for bucketname in list_buckets(s3):
                print(bucketname)
                for key in list_objects(s3, bucketname):
                    print(key)

    if args.getkeysize == True:
        if bname and bname is not None and kname and kname is not None:
            size = get_key_size(s3, bname, kname)
            print(size)
        else:
            print("Please specify the bucket and keyname")

    if args.download == True:
        if bname and bname is not None and kname and kname is not None:
            if args.localpath and args.localpath is not None:
                localpath = args.localpath
                if os.path.exists(localpath):
                    print('path {localpath} exists. \
                    please move it out of the way')
                    raise SystemExit()
                else:
                    download_key(s3, bname, kname, localpath)
            else:
                download_key(s3, bname, kname)
