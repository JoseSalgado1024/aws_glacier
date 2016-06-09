#!/usr/bin/python
# encoding: utf-8


import boto
import re
from errHandling import *

class Glacier(object):
    """
    UNA CLASE...
    TODO.
    """
    def __init__(self,
                access_key_id=None,
                secret_access_key=None,
                region_name='us-east-1',
                bucket=None,
                name_regex=None,
                restore_for=5,
                logs=False):
        """
        TODO
        """
        self.logs_enabled = logs
        if secret_access_key == None or secret_access_key == None or region_name == None:
            raise BadAuthData()
        else:
            setattr(Glacier, '_ACCESS_KEY_ID', access_key_id)
            setattr(Glacier, '_SECRET_ACCESS_KEY', secret_access_key)
            setattr(Glacier, '_REGION_NAME', region_name)

        if bucket == None or name_regex == None:
            raise BadFileOrBucket(bucket, name_regex)
        else:
            setattr(Glacier, '_BUCKET_NAME', bucket)
            setattr(Glacier, '_NAME_REGEX', name_regex)
            setattr(Glacier, '_STORE_CLASS', 'GLACIER')

        if not type(restore_for) is int or restore_for not in range (1,10):
            raise BadRestoreDays
        else:
            setattr(Glacier,
                    '_RESTORE_FOR',
                    restore_for)
        try:
            setattr(Glacier,
                    '_s3',
                    boto.connect_s3(aws_access_key_id=self._ACCESS_KEY_ID,
                                    aws_secret_access_key=self._SECRET_ACCESS_KEY))
        except Exception as e:
            raise BadAuthData(self._ACCESS_KEY_ID,
                              self._SECRET_ACCESS_KEY,
                              self._REGION_NAME)
        try:
            setattr(Glacier,
                    'selected_bucket',
                    self._s3.get_bucket(self._BUCKET_NAME))
        except Exception as e:
            raise BucketNotExists

    def _log(self, msg, error=False):
        if self.logs_enabled:
            print '{tE}: {m}'.format(m=msg, tE= 'ERROR' if error else 'INFO')

    def restore_file(self, filename, days=None):
        """TODO"""
        d = self._RESTORE_FOR if days == None else days
        if re.search(self._NAME_REGEX, filename) == None:
            self._log('\"filename\", mal formado. [{f}]'.format(f=filename), True)
            return False
        try:
            key = self.selected_bucket.get_key(filename)
        except Exception:
            self._log('No existe el archivo: {f}'.format(f=filename), True)
            return False
        try:
            key.restore(days=d)
        except Exception:
            self._log('La clase de \"{f}\" no es \"GLACIER\"'.format(f=filename), True)
            return False
        return True

    def restore_list_of_files(self, list_of_files=None, dowload=False):
        """TODO."""
        if list_of_files==None or not type(list_of_files) is list:
            raise BadFileListToRestore
        else:
            for filename in list_of_files:
                if self.restore_file(filename):
                    if self.is_available(filename):
                        self._log('Esta disponible el archivo \"{f}\" y puede ser descargado..'.format(f=filename))
                    else:
                        self._log('El archivo {f}, aun no puede ser descargado..'.format(f=filename))
                else:
                    self._log('Imposible restaurar: \"{f}\".'.format(f=filename), True)
        return True

    def is_available(self, filename):
        """TODO."""
        key = self.selected_bucket.get_key(filename)
        return key.ongoing_restore
