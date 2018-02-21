#!/usr/bin/env python
# -*- coding: utf-8 -*-
# BAREOS - Backup Archiving REcovery Open Sourced
#
# Copyright (C) 2014-2014 Bareos GmbH & Co. KG
#
# This program is Free Software; you can redistribute it and/or
# modify it under the terms of version three of the GNU Affero General Public
# License as published by the Free Software Foundation, which is
# listed in the file LICENSE.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301, USA.
#
# Author: Maik Aussendorf
#
# Bareos python plugins class that adds files from a local list to
# the backup fileset
###
### Edited and adjusted to be used for s3 backups by Me (Sean Sullivan)
###

import bareosfd
from bareos_fd_consts import bJobMessageType, bFileType, bRCs, bIOPS
import os
import re
import BareosFdPluginBaseclass
import boto3
from botocore.client import Config
import json
import io
import hashlib
import sys
from boto3_downloader import getcreds, conngen, chunky_download, list_buckets, list_objects, get_key_size, check_key_exists, check_bucket_exists, get_key_mtime


class BareosFdS3(BareosFdPluginBaseclass.BareosFdPluginBaseclass):  # noqa
    '''
    Simple Bareos-FD-Plugin-Class that parses a file and backups all files
    listed there Filename is taken from plugin argument 'filename'
    '''

    def __init__(self, context, plugindef):
        bareosfd.DebugMessage(
            context, 100,
            "Constructor called in module {self_name} with plugindef={plugin_def}\n".format(
            self_name = __name__, plugin_def = plugindef))
        # Last argument of super constructor is a list of mandatory arguments
        super(BareosFdS3, self).__init__(context, plugindef, ['configfile'])
        self.bucket_key_tuple = []
        self.bucket_list = []
        self.conn = None
        self.bucketname = None
        self.keyname = None
        self.s3config = {}
        self.restore_data = {}

#-----------------------------------------------------------------------------
#-----------------------------------------------------------------------------
    '''
    I don't see parse_configfile mentioned anywhere in BareosFdPluginBaseClass.
    I don't know if this is still needed. I'm stealing the bareos_percona one:
        parse_plugin_definition(self,context, plugindef)
    The percona one seems to use the same definitions as the BaseClass.
    '''
    def parse_plugin_definition(self, context, plugindef):
        BareosFdPluginBaseclass.BareosFdPluginBaseclass.parse_plugin_definition(self, context, plugindef)
        if 'configfile' in self.options:
            config_path = self.options['configfile']
            if os.path.exists(config_path):
                try:
                    self.config = json.load(open(config_path, 'r'))
                except ValueError as e:
                    bareosfd.JobMessage(context, bJobMessageType['M_FATAL'],
                            "s3 config file ( {config_path} ) is not valid json \
                            {error}".format(config_path = config_path, error = e))
                    return bRCs['bRC_Error']
            else:
                bareosfd.JobMessage(context, bJobMessageType['M_FATAL'],
                        "The config file mentioned can't be found \
                        {config_path}".format(config_path = config_path))
                return bRCs['bRC_Error']
            self.s3config = { "access_key": self.config['access_key'],
                              "secret_key": self.config['secret_key'],
                              "host": self.config['host'],
                              "is_secure": self.config['is_secure'],
                              "signature": self.config['signature'],
                              "astyle": self.config['astyle']
                            }
            try:
                self.conn = conngen(self.s3config)
            except Exception as e:
                bareosfd.JobMessage(context, bJobMessageType['M_FATAL'],
                        "We can't connect to S3 with the current config. \n \
                        {error}".format(error = e))
                return bRCS['bRC_Error']
#            '''Generate a list of keys for each bucket and pair them together
#            '''
            if 'bucket_list' in self.config:
                self.bucket_list = self.config['bucket_list']
            else:
                self.bucket_list = []
            if self.bucket_list and self.bucket_list is not None:
                for bucket in self.bucket_list:
                    if check_bucket_exists(self.conn, bucket):
                        for key in list_objects(self.conn, bucket):
                            self.bucket_key_tuple.append((bucket, key))
#            '''If you don't specify any buckets. We add them all
#            sans buckets in exclude_buckets list'''
            else:
                for bucket in list_buckets(self.conn):
                    if 'exclude_buckets' in  self.config:
                        if bucket in self.config['exclude_buckets']:
                            continue
                        else:
                            self.bucket_list.append(bucket)
                            for key in list_objects(self.conn, bucket):
                                self.bucket_key_tuple.append((bucket, key))
        return bRCs['bRC_OK']


    def start_backup_file(self,context, savepkt):
        '''
        This method is called, when Bareos is ready to start backup a file
        For each database to backup we create a mysqldump subprocess, wrting to
        the pipe self.stream.stdout
        '''
        bareosfd.DebugMessage(context, 100, "start_backup called\n");
        if not self.bucket_key_tuple:
            baoreosfd.DebugMessage(context,100,"No buckets to backup")
            bareosfd.JobMessage(context, bJobMessageType['M_ERROR'], "No buckets to backup.\n");
            return bRCs['bRC_Skip']

        bucketname, keyname = self.bucket_key_tuple.pop()
        keysize = get_key_size(self.conn, bucketname, keyname)
        keymtime = get_key_mtime(self.conn, bucketname, keyname)


        statp = bareosfd.StatPacket()
        if not keysize == "NULL\n":
            try:
                statp.size = int(keysize)
            except ValueError:
                pass
        statp.atime = 0
        statp.ctime = 0
        statp.mtime = keymtime
        savepkt.statp = statp
        savepkt.fname = "/{bname}/{kname}".format(bname=bucketname, kname = keyname)
        savepkt.type = bFileType['FT_REG']
        bareosfd.DebugMessage(context, 100, "Attempting to download key: '" + bucketname + "/" + keyname + "\n")
        self.stream = chunky_download(self.conn, bucketname, keyname)

        bareosfd.JobMessage(context, bJobMessageType['M_INFO'], "Starting backup of " + savepkt.fname + "\n");
        return bRCs['bRC_OK'];


    def plugin_io(self, context, IOP):
        '''
        Called for io operations. We read from pipe into buffers or on restore
        create a file for each database and write into it. Stole from Mysql plugin.
        '''
        bareosfd.DebugMessage(context, 100, "plugin_io called with " + str(IOP.func) + "\n");

        if IOP.func == bIOPS['IO_OPEN']:
            try:
                if IOP.flags & (os.O_CREAT | os.O_WRONLY):
                    self.file = open(IOP.fname, 'wb');
            except Exception as msg:
                IOP.status = -1;
                bareosfd.DebugMessage(context, 100, "Error opening file: " + IOP.fname + "\n");
                return bRCs['bRC_Error'];
            return bRCs['bRC_OK']

        elif IOP.func == bIOPS['IO_READ']:
            IOP.buf = bytearray(IOP.count)
            IOP.status = self.stream.readinto(IOP.buf)
            IOP.io_errno = 0
            return bRCs['bRC_OK']

        elif IOP.func == bIOPS['IO_WRITE']:
            try:
                self.file.write(IOP.buf);
                IOP.status = IOP.count
                IOP.io_errno = 0
            except IOError as msg:
                IOP.io_errno = -1
                bareosfd.DebugMessage(context, 100, "Error writing data: " + msg + "\n");
            return bRCs['bRC_OK'];

        elif IOP.func == bIOPS['IO_CLOSE']:
            if self.file:
                self.file.close()
            return bRCs['bRC_OK']

        elif IOP.func == bIOPS['IO_SEEK']:
            return bRCs['bRC_OK']

        else:
            bareosfd.DebugMessage(context,100,"plugin_io called with unsupported IOP:"+str(IOP.func)+"\n")
            return bRCs['bRC_OK']


    def end_backup_file(self, context):
        '''
        Check, if dump was successfull.
        '''
        if self.bucket_key_tuple:
                return bRCs['bRC_More']
        else:
            if returnCode == 0:
                return bRCs['bRC_OK'];
            else:
                return bRCs['bRC_Error']
