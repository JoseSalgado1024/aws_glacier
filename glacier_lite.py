#!/usr/bin/python
# encoding: utf-8

"""
        .-.
       /_ _\
       |o^o|
       \ _ /
      .-'-'-.
    /`)  .  (`\
   / /|.-'-.|\ \
   \ \| (_) |/ /  .-""-.
    \_\'-.-'/_/  /[] _ _\
    /_/ \_/ \_\ _|_o_LII|_
      |'._.'|  / | ==== | \
      |  |  |  |_| ==== |_|
       \_|_/    ||" ||  ||
       |-|-|    ||LI  o ||
   jgs |_|_|    ||'----'||
      /_/ \_\  /__|    |__\
"""

from libs.glacier import Glacier
from libs.configs import  my_conf
import argparse
import os
import csv

def get_args():
    """Funcion encargada de cargar los argumentos requeridos para el script."""
    parser = argparse.ArgumentParser(
        description='Script para bajar material de GLACIER')
    parser.add_argument('-f',
                        '--datafile',
                        type=str,
                        help=' CSV que contiene lista de archivos restaurar',
                        required=True)
    parser.add_argument('-l',
                        '--log',
                        type=str,
                        help='Path log file',
                        required=False)
    args = parser.parse_args()
    datafile = args.datafile
    log = args.log
    return datafile, log


def main():
    datafile, log = get_args()
    try:
        glacier = Glacier(bucket=my_conf.BUCKET_NAME,
                          access_key_id=my_conf.ACCESS_KEY_ID,
                          secret_access_key=my_conf.SECRET_ACCESS_KEY,
                          region_name=my_conf.REGION,
                          name_regex=my_conf.NAME_REGEX,
                          logs=True)
    except Exception as e:
        print e
        exit(1)

    if not os.path.exists(datafile):
        print 'ERROR: El Archivo: {f} no existe...'.format(f=datafile)
        exit(1)
    data = csv.DictReader(open(datafile))
    to_download = []
    for line in data:
        storage_class = line['storage_class'].replace(' ', '')
        filename = line['filename'].replace(' ', '')
        if storage_class == 'GLACIER':
            print 'file: \"{f}\" Added to queue , (storage_class=\"{c}\")'.format(f=filename,
                                                                                  c=storage_class)
            to_download.append(filename)
        else:
            print 'file: \"{f}\" Skyped, storage_class=\"{c}\"'.format(f=filename,
                                                                       c=storage_class)
    glacier.restore_list_of_files(to_download)



if __name__ == '__main__':
    main()
