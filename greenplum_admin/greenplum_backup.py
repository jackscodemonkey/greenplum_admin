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
from datetime import datetime


class Backup:

    def __init__(self, config, debug=None):
        loglevel=logging.ERROR

        self.debug = debug
        self.config = click.format_filename(config)
        self.logger = logging.getLogger(__name__)

        if self.debug:
            loglevel=logging.DEBUG

        self.setup_logging(loglevel)

    def setup_logging(self, loglevel):
        """
        Sets the basic config for the logger.
        """
        logformat = "[%(levelname)s]:%(asctime)s - %(message)s"
        logging.basicConfig(level=loglevel, stream=sys.stdout,
                            format=logformat, datefmt="%Y-%m-%d %H:%M%S")

    def readconfig(self, file):
        """
        Reads the config file

        Parameters
        ----------
        file : string
            Description the relative path to the config file.

        Returns
        -------
        An instance of ConfigParser
        """

        self.logger.debug('Reading config file: {}'.format(file))
        config = ConfigParser.ConfigParser()
        config.read(file)

        return config

    def get_database_list(self, configs):
        """
        Used to get a dynamic list of databases on the cluster that are allowed connections.

        Parameters
        ----------
        configs : instance of ConfigParser
            Description all the config values in or configuration file.

        Returns
        -------
        list
        """
        _return_records=[]
        _backup_user_db = configs.get('backup','backup_user_db')
        _connection_db = configs.get('backup','connection_db')
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

    def build_backup_command(self,configs, db_list):
        """
        Builds a list of backup commands that will be executed.

        Parameters
        ----------
        configs : instance of ConfigParser
            Description all the config values in or configuration file.
        db_list
            Description either the dynamic list of databases on the cluster or from the config file.

        Returns
        -------
        list
        """
        command_list=[]
        _today = datetime.now().strftime('%a')
        self.logger.debug('Day: {}'.format(_today).lower())
        _options = configs.get('backup','backup_options')
        _master_data_dir = configs.get('backup','greenplum_master_directory')
        _backup_path = configs.get('backup','backup_path')
        _backup_day = configs.get('backup', _today)

        if _backup_day != 'none':
            for db in db_list:
                _backup_folder = '{}/{}'.format(_backup_path,db)
                if _backup_day == 'full':
                    _backup_type = ''
                else:
                    _backup_type = _backup_day

                cmd = 'gpcrondump -x {} {} -d {} -l {}/logs -u {} {}'.format(db,_options,_master_data_dir,_backup_folder,_backup_folder,_backup_type)
                command_list.append({_backup_folder:cmd})

        return command_list

    def build_backup_dir(self, directory):
        if not os.path.isdir(directory):
            os.makedirs(directory)

    def execute_backups(self,backup_commands):
        for item in backup_commands:
            dir = item.keys()[0]
            cmd = item[dir]
            self.build_backup_dir(dir)

    def run(self):
        """ Primary method to control program flow """
        configs = self.readconfig(self.config)
        _backup_user = configs.get('backup','backup_user_os')
        _backup_path = configs.get('backup', 'backup_path')
        _backup_database_list = map(str.strip,configs.get('backup','backup_database_list').split(','))

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
                dbs = self.get_database_list(configs)
            else:
                dbs = _backup_database_list
            self.logger.debug(list(dbs))

            """ Generate the backup commands for gpcrondump """
            backup_cmds = self.build_backup_command(configs,dbs)
            self.logger.debug('Backup commands: {} '.format(backup_cmds))

            """ Execute backups """
            self.execute_backups(backup_cmds)

        else:
            self.logger.error('Backup path does not exist. {}'.format(_backup_path))
            sys.exit(-1)


@click.command()
@click.option('--config', '-c', required=True, help='Path to a configuration file.', type=click.Path(exists=True))
@click.option('--debug', '-v', flag_value='DEBUG', help='Verbose logging output')
def main(config,debug):
    prog = Backup(config,debug)
    prog.run()


if __name__ == "__main__":
    main()
