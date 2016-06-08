import os
import ConfigParser

# Hidden configs file
CONFIG_FILE =  os.path.expanduser('.glacier.conf')


# configs keys
CONFIG_KEY = {
    'AWS_ACCESS_KEY_ID': 'AWS_ACCESS_KEY_ID',
    'AWS_SECRET_ACCESS_KEY': 'AWS_SECRET_ACCESS_KEY',
    'AWS_REGION': 'AWS_REGION'
}
# Load configs values





Name = ConfigSectionMap("SectionOne")['name']


class MyConf(object):
    def __init__(self,
                 filename=None,
                 logs=False):
        self._enable_logs = logs
        self.AWS_ACCESS_KEY_ID = ''
        self.AWS_SECRET_ACCESS_KEY = ''
        self.AWS_REGION = ''
        self._CFG_FILE = filename
        self._Config = ConfigParser.ConfigParser()
        self._CONFIG_SEC = [{'name': 'AWS_CREDENTIALS',
                             'cfg_keys': ['AWS_ACCESS_KEY_ID',
                                          'AWS_SECRET_ACCESS_KEY',
                                          'AWS_REGION']}]

    def _log(self, msg):
        if True:
            print msg

    def _Load_file(self):
        try:
            self._log('INFO: CARGANDO [{cfgFile}]'.format(cfgFile=CONFIG_FILE))
            self._Config.read(conf_filename)
        except IOError:
            self._log('ERROR(FATAL): Imposible el archivo de '\
                      'configuracion: {var_name}.'.format(var_name=e))
            raise

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
            for sec self._CONFIG_KEY
        except IOError:
            exit(1)
        except AttributeError:
            self._log('No pida boludeces...')
            exit(1)

try:
    my_conf = MyConf(CONFIG_FILE, logs=True)
except Exception as e:
    print 'fallo carga de configuracion...'
