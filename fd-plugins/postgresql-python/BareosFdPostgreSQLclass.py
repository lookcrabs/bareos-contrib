#!/usr/bin/env python
# -*- coding: utf-8 -*-

# originally contributed by:
#Copyright 2014 Battelle Memorial Institute
#Written by Evan Felix

# With additions from Maik Aussendorf, Bareos GmbH & Co. KG 2015

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

## Modified by Sean Sullivan on 2018. Blah Blah. Remove this line

import bareosfd
from bareos_fd_consts import bJobMessageType, bFileType, bRCs, bIOPS
import os
from subprocess import Popen, PIPE
import BareosFdPluginBaseclass


class BareosFdPostgreSQLclass (BareosFdPluginBaseclass.BareosFdPluginBaseclass):
    '''
        Plugin for backing up all postgres databases found in a specific postgres server
    '''       
    def __init__(self, context, plugindef):
        bareosfd.DebugMessage(
            context, 100,
            "Constructor called in module {self_name} with plugindef={plugin_def}\n".format(
            self_name = __name__, plugin_def = plugindef))
        # Last argument of super constructor is a list of mandatory arguments
        super(BareosFdPostgreSQLclass, self).__init__(context, plugindef)
        self.file = None


    def parse_plugin_definition(self,context, plugindef):
        '''
        '''
        BareosFdPluginBaseclass.BareosFdPluginBaseclass.parse_plugin_definition(self, context, plugindef)

        # pgsql host and credentials, by default we use localhost and root and
        # prefer to have a my.cnf with pgsql credentials
        self.pgsqlconnect = ''

        if 'dumpbinary' in self.options:
            self.dumpbinary = self.options['dumpbinary']
        else:
            self.dumpbinary = "/usr/bin/pg_dump"

        # if dumpotions is set, we use that completely here, otherwise defaults
        if 'dumpoptions' in self.options:
            self.dumpoptions = self.options['dumpoptions']
        else:
            self.dumpoptions = " --format=p --blobs"
            # default is to add the drop statement
            if not 'drop_and_recreate' in self.options or not self.options['drop_and_recreate'] == 'false':
                self.dumpoptions += " --clean"

        # if defaultsfile is set
        if 'defaultsfile' in self.options:
            self.defaultsfile = self.options['defaultsfile']
            self.pgsqlconnect += " --defaults-file=" + self.defaultsfile

        if 'pgsqlhost' in self.options:
            self.pgsqlhost = self.options['pgsqlhost']
            self.pgsqlconnect += " --host=" + self.pgsqlhost

        if 'pgsqluser' in self.options:
            self.pgsqluser = self.options['pgsqluser']
            self.pgsqlconnect += " --username=" + self.pgsqluser

        if 'pgsqlpassword' in self.options:
            self.pgsqlpassword = self.options['pgsqlpassword']
            self.pgsqlconnect += " --password=" + self.pgsqlpassword

        # if plugin has db configured (a list of comma separated databases to backup
        # we use it here as list of databases to backup
        if 'db' in self.options:
            self.databases = self.options['db'].split(',')
        # Otherwise we backup all existing databases
        else:
            showDbCommand = "sudo -u postgres psql {sqlopts} -At -c 'select datname from pg_database where not datistemplate and datallowconn order by datname;'".format(sqlopts = self.pgsqlconnect)
            showDb = Popen(showDbCommand, shell=True, stdout=PIPE, stderr=PIPE)
            self.databases = showDb.stdout.read().splitlines()
            if 'performance_schema' in self.databases:
                self.databases.remove('performance_schema')
            if 'information_schema' in self.databases:
                self.databases.remove('information_schema')
            showDb.wait()
            returnCode = showDb.poll()
            if returnCode == None:
                bareosfd.JobMessage(context, bJobMessageType['M_FATAL'], "No databases specified and show databases failed for unknown reason");
                bareosfd.DebugMessage(context, 10, "Failed pgsql command: '%s'" %showDbCommand)
                return bRCs['bRC_Error'];
            if returnCode != 0:
                (stdOut, stdError) = showDb.communicate()
                bareosfd.JobMessage(context, bJobMessageType['M_FATAL'], "No databases specified and show databases failed. %s" %stdError);
                bareosfd.DebugMessage(context, 10, "Failed pgsql command: '%s'" %showDbCommand)
                return bRCs['bRC_Error'];

        if 'ignore_db' in self.options:
            bareosfd.DebugMessage(context, 100, "databases in ignore list: %s\n" %(self.options['ignore_db'].split(',')));
            for ignored_cur in self.options['ignore_db'].split(','):
                try:
                    self.databases.remove(ignored_cur)
                except:
                    pass
        bareosfd.DebugMessage(context, 100, "databases to backup: %s\n" %(self.databases));
        return bRCs['bRC_OK'];


    def start_backup_file(self,context, savepkt):
        '''
        This method is called, when Bareos is ready to start backup a file
        For each database to backup we create a pgsqldump subprocess, wrting to
        the pipe self.stream.stdout
        '''
        bareosfd.DebugMessage(context, 100, "start_backup called\n");
        if not self.databases:
            bareosfd.DebugMessage(context,100,"No databases to backup")
            bareosfd.JobMessage(context, bJobMessageType['M_ERROR'], "No databases to backup.\n");
            return bRCs['bRC_Skip']

        db = self.databases.pop()

        sizeDbCommand = 'sudo -u postgres /usr/bin/psql -At -c "select pg_database_size(\'{database_name}\')"'.format(database_name = db)
        sizeDb = Popen(sizeDbCommand, shell=True, stdout=PIPE, stderr=PIPE)
        size_curr_db = sizeDb.stdout.read()
        sizeDb.wait()
        sizereturnCode = sizeDb.poll()

        statp = bareosfd.StatPacket()
        if not size_curr_db == "NULL\n":
            try:
                statp.size = int(size_curr_db)
            except ValueError:
                pass
        savepkt.statp = statp
        savepkt.fname = "/_pgsqlbackups_/"+db+".sql"
        savepkt.type = bFileType['FT_REG']

        dumpcommand = ("sudo -u postgres {dumpcmd} {connectopts} {dumpopts} {database}".format(
                                                                       dumpcmd = self.dumpbinary,
                                                                       connectopts = self.pgsqlconnect,
                                                                       database = db,
                                                                       dumpopts =  self.dumpoptions))
        bareosfd.DebugMessage(context, 100, "Dumper: '" + dumpcommand + "'\n")
        self.stream = Popen(dumpcommand, shell=True, stdout=PIPE, stderr=PIPE)

        bareosfd.JobMessage(context, bJobMessageType['M_INFO'], "Starting backup of " + savepkt.fname + "\n");
        return bRCs['bRC_OK'];


    def plugin_io(self, context, IOP):
        '''
        Called for io operations. We read from pipe into buffers or on restore
        create a file for each database and write into it.
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
            IOP.status = self.stream.stdout.readinto(IOP.buf)
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
        # Usually the pgsqldump process should have terminated here, but on some servers
        # it has not always.
        self.stream.wait()
        returnCode = self.stream.poll()
        if returnCode == None:
            bareosfd.JobMessage(context, bJobMessageType['M_ERROR'], "Dump command not finished properly for unknown reason")
            returnCode = -99
        else:
            bareosfd.DebugMessage(context, 100, "end_backup_file() entry point in Python called. Returncode: %d\n" %self.stream.returncode)
            if returnCode != 0:
                (stdOut, stdError) = self.stream.communicate()
                if stdError == None:
                    stdError = ''
                bareosfd.JobMessage(context, bJobMessageType['M_ERROR'], "Dump command returned non-zero value: %d, message: %s\n" %(returnCode,stdError));

        if self.databases:
                return bRCs['bRC_More']
        else:
            if returnCode == 0:
                return bRCs['bRC_OK'];
            else:
                return bRCs['bRC_Error']


# vim: ts=4 tabstop=4 expandtab shiftwidth=4 softtabstop=4
