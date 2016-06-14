#!/usr/bin/python
# encoding: utf-8
import os
import ConfigParser
from screen_prints import *


# Hidden configs file
CONFIG_FILE = os.path.join(os.path.dirname(__file__),
                           '.glacier_foo.cfg')
my_conf = None

# configs keys
CONFIG_KEY = {
    'AWS_ACCESS_KEY_ID': 'AWS_ACCESS_KEY_ID',
    'AWS_SECRET_ACCESS_KEY': 'AWS_SECRET_ACCESS_KEY',
    'AWS_REGION': 'AWS_REGION'
}


class MyConf(object):
    def __init__(self,
                 filename=None,
                 logs=False):
        self._enable_logs = logs
        self._CFG_FILE = filename
        self._Config = ConfigParser.ConfigParser()
        self._CONFIG_SEC = [{'name': 'GENERAL',
                             'cfg_keys': ['ACCESS_KEY_ID',
                                          'SECRET_ACCESS_KEY',
                                          'REGION',
                                          'VAULT_NAME',
                                          'BUCKET_NAME',
                                          'NAME_REGEX']}]

    def _log(self, msg, error=False):
        if self._enable_logs:
            print '{typeE}: {msg}'.format(msg=msg,
                                          typeE='ERROR' if error else 'INFO')

    def _Load_file(self):
        self._log(mensages.LOADING_CONF.format(f=self._CFG_FILE))
        try:
            self._Config.read(self._CFG_FILE)
            self._log(mensages.FILE_LOAD_FAIL.format(f=self._CFG_FILE))
        except IOError:
            self._log(mensages.CONF_FILE_LOAD_FAIL.format(f=self._CFG_FILE),
                      True)
            raise IOError

    def _ConfigSectionMap(self, section):
        dict1 = {}
        options = self._Config.options(section)
        for option in options:
            try:
                dict1[option] = self._Config.get(section, option)
                if dict1[option] == -1:
                    self._log("skip: %s" % option)
            except:
                self._log("exception on %s!" % option)
                dict1[option] = None
        return dict1

    def load(self):
        try:
            self._Load_file()
        except IOError:
            raise IOError

        for sec in self._CONFIG_SEC:
            self._log('loading conf for {sec}'.format(sec=sec['name']))
            for k in sec['cfg_keys']:
                self._log('Conf property: {property}'.format(property=k))
                try:
                    setattr(MyConf,
                            k,
                            self._ConfigSectionMap(sec['name'])[k.lower()])
                except Exception as e:
                    self._log('NO EXISTE...', True)

my_conf = MyConf(CONFIG_FILE)
my_conf.load()

try:
    my_conf = MyConf(CONFIG_FILE)
    my_conf.load()
except Exception as e:
    print mensages.CONF_FILE_LOAD_FAIL.format(f=CONFIG_FILE)
    exit(1)
