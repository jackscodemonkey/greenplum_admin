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


class Backup:

    def __init__(self, config, verbose=None,debug=None):
        loglevel=logging.ERROR

        self.verbose = verbose
        self.debug = debug
        self.config = click.format_filename(config)
        self.logger = logging.getLogger(__name__)

        if self.verbose:
            loglevel=logging.INFO
        elif self.debug:
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

    def run(self):
        """ Primary method to control program flow """
        configs = self.readconfig(self.config)
        backup_user = configs.get('backup','backup_user')
        backup_path = configs.get('backup', 'backup_path')

        if not backup_user == pwd.getpwuid(os.getuid())[0]:
            file_name = os.path.basename(sys.argv[0])
            self.logger.error('{} can only be executed as: {}'.format(file_name,backup_user))
            sys.exit(-1)
        if os.path.isdir(backup_path):
            self.logger.debug('Found found backup path: {}'.format(backup_path))
        else:
            self.logger.error('Backup path does not exist. {}'.format(backup_path))
            sys.exit(-1)


@click.command()
@click.option('--config', '-c', required=True, help='Path to a configuration file.', type=click.Path(exists=True))
@click.option('--verbose', '-v', flag_value='INFO', help='Verbose logging output')
@click.option('--debug', '-vv', flag_value='DEBUG', help='Verbose logging output')
def main(config,verbose,debug):
    prog = Backup(config,verbose,debug)
    prog.run()


if __name__ == "__main__":
    main()
