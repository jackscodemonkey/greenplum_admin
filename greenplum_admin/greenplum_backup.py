#!/usr/bin/evn python
# -*- coding: utf-8 -*-
"""
Backup wrapper for Greenplum backups to control backup
rotation and cleanup
"""

import sys
import os
import pwd
import click
import logging
import ConfigParser
import psycopg2
import re
from datetime import datetime
from collections import defaultdict
from subprocess import call
from shutil import rmtree


class Backup:

    def __init__(self, config, debug=None):

        loglevel=logging.ERROR

        self.debug = debug
        self.config = click.format_filename(config)
        self.logger = logging.getLogger(__name__)
        self.logger.propagate = 0

        if self.debug:
            loglevel=logging.DEBUG

        """ Load the config file """
        self.configs = self.readconfig(self.config)

        self.setup_logging(loglevel)

    def setup_logging(self, loglevel):
        """
        Sets the basic config for the logger.
        """

        _log_dir = self.configs.get('backup', 'log_directory')
        logging.basicConfig(level=loglevel)

        _logformatter = logging.Formatter("[%(levelname)s]:%(asctime)s - %(message)s")
        _rootlogger = self.logger

        if not _rootlogger.handlers:
            if _log_dir:
                _stamp = datetime.now().strftime('%Y%m%d%H%M%S')
                _file_name = os.path.basename(sys.argv[0])
                _log_file = '{}/{}_{}.log'.format(_log_dir, _file_name, _stamp)

                filehandler = logging.FileHandler(_log_file)
                filehandler.setFormatter(_logformatter)
                _rootlogger.addHandler(filehandler)

            consolehandler = logging.StreamHandler()
            consolehandler.setFormatter(_logformatter)
            _rootlogger.addHandler(consolehandler)

    def readconfig(self, config_file):
        """
        Reads the config file

        Parameters
        ----------
        config_file : string
            Description the relative path to the config file.

        Returns
        -------
        An instance of ConfigParser
        """

        self.logger.debug('Reading config file: {}'.format(file))
        config = ConfigParser.ConfigParser()
        config.read(config_file)

        return config

    def get_database_list(self):
        """
        Used to get a dynamic list of databases on the cluster that are allowed connections.


        Returns
        -------
        list
        """
        _return_records=[]
        _backup_user_db = self.configs.get('backup', 'backup_user_db')
        _connection_db = self.configs.get('backup', 'connection_db')
        connection_string="host=localhost dbname={} user={}".format(_connection_db,_backup_user_db)
        try:
            conn = psycopg2.connect(connection_string)
            cursor = conn.cursor()
            cursor.execute("SELECT datname FROM pg_database WHERE datallowconn='t' order by 1")
            records = cursor.fetchall()
            for item in records:
                _return_records.append(item[0])

        except Exception as e:
            self.logger.error("Error looking up databases: {}".format(e.message))

        return _return_records

    def build_backup_command(self, db_list):
        """
        Builds a list of backup commands that will be executed.

        Parameters
        ----------
        db_list
            Description either the dynamic list of databases on the cluster or from the config file.

        Returns
        -------
        list
        """
        command_list = []
        _today = datetime.now().strftime('%a')
        self.logger.debug('Day: {}'.format(_today).lower())
        _options = self.configs.get('backup', 'backup_options')
        _master_data_dir = self.configs.get('backup', 'greenplum_master_directory')
        _backup_path = self.configs.get('backup', 'backup_path')
        _backup_day = self.configs.get('backup', _today)

        if _backup_day != 'none':
            for db in db_list:
                _backup_folder = '{}/{}'.format(_backup_path,db)
                if _backup_day == 'full':
                    _backup_type = ''
                else:
                    _backup_type = _backup_day

                cmd = ' -x {} {} -d {} -l {}/logs -u {} {}'.format(db, _options, _master_data_dir,
                                                                   _backup_folder, _backup_folder,
                                                                   _backup_type)
                command_list.append({_backup_folder: cmd})

        return command_list

    @staticmethod
    def build_backup_dir(directory):
        if not os.path.isdir(directory):
            os.makedirs(directory)
            return False
        else:
            return True

    def execute_backups(self, backup_commands):
        for item in backup_commands:
            _dir = item.keys()[0]
            _cmd = item[_dir]

            """ Append the log directory to our built path and create the new structure. """
            _dir = '{}/{}'.format(_dir, 'logs')

            _backup_program = self.configs.get('backup', 'backup_program')
            if not self.build_backup_dir(_dir):
                """ Backup directory was just created, we should over-ride incremental backups and run a full
                backup or the backup will fail."""
                _cmd = str(_cmd).replace('incremental', '')
                self.logger.debug('Backup command: {}{}'.format(_backup_program, _cmd))
            else:
                self.logger.debug('Backup command: {}{}'.format(_backup_program, _cmd))

            """ Execute the backup """
            try:
                call([_backup_program, _cmd])
            except OSError as e:
                self.logger.error('Failed to execute the backup. {}'.format(e.strerror))
                sys.exit(-1)

            """ Read the gp log files for this database and get information rotation cleanup."""
            _dict_logs = self.read_gp_log(_dir)

            """ Find the directories that are older than then number of full backups we keep """
            _remaining = []
            for key in _dict_logs:
                self.logger.debug("Database log contents: Key {} ".format(key))
                _counter = int(self.configs.get('backup', 'keep_full_backups'))
                _oldest_full = []

                for x in _dict_logs[key]:
                    if str(x[2]).lower() == "full database":
                        _counter -= 1
                        if _counter == 0:
                            _oldest_full.append(x)

                        if _counter < 0:
                            _remaining.append(x)

                    elif _counter == 0:
                        _remaining.append(x)

                self.logger.debug("Oldest full: {}".format(_oldest_full))
                self.logger.debug("Remaining: {}".format(_remaining))

                """ delete old backups """
                self.delete_old_backups(key, _remaining)

    def delete_old_backups(self, database, del_list):
        """
        Deletes the old database backup folders and removes the corresponding log file.
        Errors are non-fatal, we don't want to stop backups if we can't remove a directory.
        Monitoring should pick up that the backup drive is filling up.

        Parameters
        ----------
        database : string
            Description name of the database to delete old files for

        del_list : list
            Description list of directory names and log files to delete

        Returns
        -------
        bool
        """
        self.logger.debug("Delete List: {}".format(del_list))
        for x in del_list:
            _del_log = x[0]
            _del_dir = '{}/{}/db_dumps/{}'.format(self.configs.get('backup', 'backup_path'), database, x[1])

            if os.path.exists(_del_dir):
                try:
                    rmtree(_del_dir)
                    self.logger.debug('Deleted directory: {}'.format(_del_dir))

                    if os.path.exists(_del_log):
                        try:
                            os.remove(_del_log)
                            self.logger.debug('Deleted log file: {}'.format(_del_log))
                        except OSError as e:
                            self.logger.error(
                                'Error deleting log file: {}. Error: {} {}'.format(_del_log, e.errno, e.strerror))

                except OSError as e:
                    self.logger.error('Error deleting directory: {}. Error: {} {}'.format(_del_dir, e.errno, e.strerror))
            else:
                self.logger.warning('Backup folder does not exist but log file does!:  {} {}'.format(_del_dir, _del_log))

    def read_gp_log(self, log_folder_path):
        """
        Parse the backup logs to find the type of backup and backup folder.
        The backup folder is always automatically named by gpcrondump with the date the backup was executed;
        this is useful for determining folders to clean up.

        Parameters
        ----------
        log_folder_path : string
            Description path for logs in each database backup directory

        Returns
        -------
        defaultdict(list)
        """
        _dict = defaultdict(list)

        _file_list = os.listdir(log_folder_path)
        _find_db_name = re.compile(r"(Target database.*)")
        _find_dump_dir = re.compile(r"(Dump subdirectory.*)")
        _find_dump_type1 = re.compile(r"(Dump type.*)")
        _find_dump_type2 = re.compile(r"(=.*)")
        self.logger.debug("List of log files: {}".format(_file_list.sort(reverse=True)))
        for fname in _file_list:
            fullpath = '{}/{}'.format(log_folder_path, fname)
            with open(fullpath, 'r') as f:
                content = f.read()
                f.close()
                _db_name = str(_find_db_name.findall(content)[-1]).split()[-1]
                _dump_dir = str(_find_dump_dir.findall(content)[-1]).split()[-1]
                _dump_type1 = _find_dump_type1.findall(content)[-1]
                _dump_type2 = _find_dump_type2.search(_dump_type1).group(1).replace('= ', '').strip()

                self.logger.debug("Database: {}".format(_db_name))
                self.logger.debug("Dump Dir: {}".format(_dump_dir))
                self.logger.debug("Dump Type: {}".format(_dump_type1))
                self.logger.debug("Dump Type: {}".format(_dump_type2))

                _dict[_db_name].append((fullpath, _dump_dir, _dump_type2))

        return _dict

    def run(self):
        """ Primary method to control program flow """
        _backup_user = self.configs.get('backup', 'backup_user_os')
        _backup_path = self.configs.get('backup', 'backup_path')
        _backup_database_list = map(str.strip, self.configs.get('backup', 'backup_database_list').split(','))

        """ Must run with the user we're expecting """
        if not _backup_user == pwd.getpwuid(os.getuid())[0]:
            file_name = os.path.basename(sys.argv[0])
            self.logger.error('{} can only be executed as: {}'.format(file_name,_backup_user))
            sys.exit(-1)

        """ Check that our backup path exists """
        if os.path.isdir(_backup_path):
            self.logger.debug('Found found backup path: {}'.format(_backup_path))
            self.logger.debug('Length of database list from config file: {}'.format(len(_backup_database_list)))

            """ Get our database list either from the config file or from the cluster itself """
            if not len(_backup_database_list) > 0 or '' in _backup_database_list[:1]:
                dbs = self.get_database_list()
            else:
                dbs = _backup_database_list
            self.logger.debug(list(dbs))

            """ Generate the backup commands for gpcrondump """
            backup_cmds = self.build_backup_command(dbs)
            self.logger.debug('Backup commands: {} '.format(backup_cmds))

            """ Execute backups """
            self.execute_backups(backup_cmds)

        else:
            self.logger.error('Backup path does not exist. {}'.format(_backup_path))
            sys.exit(-1)


@click.command()
@click.option('--config', '-c', required=True, help='Path to a configuration file.', type=click.Path(exists=True))
@click.option('--debug', '-v', flag_value='DEBUG', help='Verbose logging output')
def main(config, debug):
    prog = Backup(config, debug)
    prog.run()


if __name__ == "__main__":
    main()
