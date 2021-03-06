#!/usr/bin/python
# encoding: utf-8
import boto
import re
from errHandling import *
from screen_prints import *


class Glacier(object):
    """
    Clase creada para Restaurar Archivos expirados en un bucket.
    Args:
    ====
        + access_key_id:
            - Recive: Id de Acceso de AWS.
            - type: Str.
        + secret_access_key:
            - Recive: Clave Secreta de Acceso de AWS.
            - type: Str.
        + region_name:
            - Recive: Nombre de la region de AWS.
            - type: Str.
        + bucket:
            - Recive: Nombre del Bucket donde estan alojados los archivos que
                      deseamos restaurar.
            - type: Str.
        + name_regex:
            - Recive: Expresion regular para validar Nombres.
            - type: Str.
        + restore_for:
            - Recive: Dias por que desea restaurar los archivos.
            - type: Int.
        + logs:
            - Recive: True | False. Activar o desactivar impresion de logs.
            - type: Boolean
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
        if None not in [secret_access_key, secret_access_key, region_name]:
            raise BadAuthData()
        else:
            setattr(Glacier, '_ACCESS_KEY_ID', access_key_id)
            setattr(Glacier, '_SECRET_ACCESS_KEY', secret_access_key)
            setattr(Glacier, '_REGION_NAME', region_name)

        if type(bucket) is None or type(name_regex) is None:
            raise BadFileOrBucket(bucket, name_regex)
        else:
            setattr(Glacier, '_BUCKET_NAME', bucket)
            setattr(Glacier, '_NAME_REGEX', name_regex)
            setattr(Glacier, '_STORE_CLASS', 'GLACIER')

        if not type(restore_for) is int or restore_for not in range(1, 10):
            raise BadRestoreDays
        else:
            setattr(Glacier,
                    '_RESTORE_FOR',
                    restore_for)
        try:
            setattr(Glacier,
                    '_s3',
                    boto.connect_s3(
                        aws_access_key_id=self._ACCESS_KEY_ID,
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
        """Funcion que imprime Logs, si los mismos estan activados.
        Args:
        ====
            + msg:
                - Recive: Mensaje de error para ser impreso.
                - Type: Str.
            + error:
                - Recive: True | False, dependiendo si es un error o solo
                  un info.
                - Type: Boolean.
        """
        if self.logs_enabled:
            print '{tE}: {m}'.format(m=msg, tE='ERROR' if error else 'INFO')

    def restore_file(self, filename, days=None):
        """
        Restaurar los archivos expirados en un bucket.
        Esta funcion restaura archivos que por lifecycle, ya no estan
        disponibles en un bucket. Chequea existencia del archivo e incluso
        que el mismo sea de la storage_class "glacier".
        return (parm): True | False.
        return (print): Si los logs estan activados, imprime por consola el
        resultado "humano" de la transacción.

        Args:
        =====
            + filename:
              - Recive: Nombre del archivo expirado.
              - Type: Str.
            + day:
              - Recive:
                 -- Sobrecarga: None
                 -- Explicito: cantidad de dias que se requiere que este
                    "restarado" en archivo dentro del bucket.
              - Type:
                 -- Sobrecarga: NoneType.
                 -- Explicito: int.
        """
        d = self._RESTORE_FOR if type(days) is None else days
        if type(re.search(self._NAME_REGEX, filename)) is None:
            self._log(mensages.BAD_FILENAME.format(f=filename), True)
            return False
        try:
            key = self.selected_bucket.get_key(filename)
        except Exception:
            self._log(mensages.FILE_NOT_EXISTS.format(f=filename), True)
            return False
        try:
            key.restore(days=d)
        except Exception:
            m = mensages.FILE_EXISTS_BUT_NOT_AVAILABLE_GLACIER
            self._log(m.format(f=filename), True)
            return False
        return True

    def restore_list_of_files(self, list_of_files=None, dowload=False):
        """
        Restaurar lista de archivos expirados en un bucket.
        Esta funcion chequea la existencia del archivo (filename) e incluso
        que el mismo sea de la storage_class "glacier".
        return (parm): True | False
        return (print): Si los logs estan activados, imprime por consola el
        resultado "humano" de la transacción.

        Args:
        =====
            + list_of_files:
              - Recive: lista de Nombres de archivo expirados.
              - Type: List.
            + dowload:
              - Recive: True o False
              - Type: Boolean
        """
        if type(list_of_files) is None or not type(list_of_files) is list:
            raise BadFileListToRestore
        else:
            for filename in list_of_files:
                if self.restore_file(filename):
                    if self.is_available(filename):
                        m = mensages.FILE_READY_FOR_DOWNLOAD
                        self._log(m.format(f=filename))
                    else:
                        m = mensages.FILE_EXISTS_BUT_NOT_AVAILABLE
                        self._log(m.format(f=filename))
                        return False
                else:
                    self._log(mensages.CANT_RESTORE_FILE.format(f=filename),
                              True)
        return True

    def dowload_s3_data(self,
                        remote_file,
                        dst_folder,
                        delete_original=False):
        """
        Descarga archivo expirados desde un bucket.
        Esta funcion chequea la existencia del archivo (remote_file).
        return (parm): True | False
        return (print): Si los logs estan activados, imprime por consola el
        resultado "humano" de la transacción.

        Args:
        =====
            + remote_file:
              - Recive: Archivo alojado en bucket.
              - Type: Str.

            + dst_folder:
              - Recive: Path al directorio donde se desea guardar el archivo
                descargado.
              - Type: Str.

            + delete_original:
              - Recive: True o False.
              - Type: Boolean.
        """
        if not os.path.exists(dst_folder):
            os.makedirs(dst_folder)
        bucket = self.selected_bucket
        try:
            local_file = os.path.join(dst_folder, remote_file)
            bucket.download_file(remote_file, local_file)
            if delete_original:
                s3.Object(bucket, remote_file).delete()
            return True
        except Exception:
            m = 's3://{bucket}/{filename} is in GLACIER'
            self._log(m.format(bucket=GLACIER,
                               filename=remote_file), True)
            return False

    def is_available(self, filename):
        """
        Chequea disponibilidad de archivo expirado en un bucket(remote_file).
        return (parm): True | False.
        return (print): Si los logs estan activados, imprime por consola el
        resultado "humano" de la transacción.

        Args:
        =====
            + remote_file:
              - Recive: Archivo alojado en bucket.
              - Type: Str.

            + dst_folder:
              - Recive: Path al directorio donde se desea guardar el archivo
                descargado.
              - Type: Str.

            + delete_original:
              - Recive: True o False.
              - Type: Boolean.
        """
        key = self.selected_bucket.get_key(filename)
        file_status = key.ongoing_restore
        msg_status = 'READY' if file_status else 'NOT-READY'
        self._log(mensages.FILE_NOT_NEED_BE_RESTORED.format(f=filename,
                                                            s=msg_status))
        return file_status
