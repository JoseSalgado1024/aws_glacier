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

import boto3
import argparse
import time
from functools import wraps
import math
from tabulate import tabulate
import re
import threading
import os
import shutil


MAX_DOWNLOAD_THREADS = 12
actual_download_thread = 0
all_download_threads = 0
all_download_threads_ok = 0
FOLDER = 'bucket_glacier'
DEFAULT_BUCKET = 'comercio-sepa-glacier'
S3 = boto3.resource('s3')
SELECTED_BUCKET = S3.Bucket(DEFAULT_BUCKET)
DEFAULT_STORAGE_CLASS = 'glacier'
VALID_FILNAME_REGEX = u'sepa_(?P<run_time>[0-9]{1,2})_comercio-sepa-(?P<comer'\
                      u'cio_id>[0-9]{1,2})_(?P<year>[0-9]{4})-(?P<month>[0-9]'\
                      u'{2})-(?P<day>[0-9]{2})_(?P<hours>[0-9]{2})-(?P<mins>['\
                      u'0-9]{2})-(?P<seconds>[0-9]{2})\.(?P<format>'\
                      u'[a-zA-Z0-9]{3})'

FILENAME_PARTS = ['format',
                  'seconds',
                  'mins',
                  'hours',
                  'day',
                  'month',
                  'year',
                  'comercio_id',
                  'run_time']
white_list = []
to_download = []
to_print = []
all_list = []
to_print_glacier = []
PROF_DATA = {}


def profile(fn):
    @wraps(fn)
    def with_profiling(*args, **kwargs):
        start_time = time.time()
        ret = fn(*args, **kwargs)
        elapsed_time = time.time() - start_time
        if fn.__name__ not in PROF_DATA:
            PROF_DATA[fn.__name__] = [0, []]
        PROF_DATA[fn.__name__][0] += 1
        PROF_DATA[fn.__name__][1].append(elapsed_time)
        return ret
    return with_profiling


def print_prof_data():
    for fname, data in PROF_DATA.items():
        max_time = max(data[1])
        avg_time = sum(data[1]) / len(data[1])
        print '\nMetricas para %s' % fname
        print '\t + Ejecuciones: {veces} veces.\n'\
              '\t + Tiempo de ejecucion promedio: {avgt}ms\n'\
              '\t + Mayor tiempo de ejecucion:'\
              ' {mt}ms'.format(veces=data[0],
                               mt=str(math.trunc(max_time)),
                               avgt=str(math.trunc(avg_time)))


def clear_prof_data():
    global PROF_DATA
    PROF_DATA = {}


@profile
def dispatch_threads_download(zip_files_list,
                              headers=False,
                              storage_class=DEFAULT_STORAGE_CLASS.upper(),
                              dst_folder=FOLDER):
    if not headers:
        get_this = zip_files_list
    else:
        get_this = zip_files_list[1:len(zip_files_list)]
    global MAX_DOWNLOAD_THREADS
    global actual_download_thread
    if not os.path.exists(FOLDER):
        os.makedirs(FOLDER)
    for zip_file in zip_files_list:
        if zip_file[3] in [storage_class.lower(), storage_class]:
            if MAX_DOWNLOAD_THREADS > actual_download_thread:
                r = re.search(VALID_FILNAME_REGEX, zip_file[0])
                df = os.path.join(FOLDER,
                                  have_all_fields_required(r,
                                                           FILENAME_PARTS,
                                                           True)['bucket'])
                if not os.path.exists(df):
                    os.makedirs(df)
                p = threading.Thread(target=dowload_s3_data,
                                     args=[**{'remote_file': zip_file[0],
                                              'dst_folder': df}])
                p.start()
            else:
                while MAX_DOWNLOAD_THREADS <= actual_download_thread:
                    time.sleep(1)
                p = threading.Thread(target=dowload_s3_data,
                                     args=[zip_file[0]])
                p.start()


@profile
def have_all_fields_required(regex_result_group,
                             vars_required,
                             dict_resp=False):
    if type(regex_result_group) is None:
        return False
    for v in vars_required:
        try:
            if type(regex_result_group.group(v)) is None:
                return False
        except Exception as e:
            return False
    if not dict_resp:
        return 'sepa_{etlr}_comercio-sepa-{cid}_{year}-{month}-'\
                '{day}'.format(cid=regex_result_group.group('comercio_id'),
                               etlr=regex_result_group.group('run_time'),
                               year=regex_result_group.group('year'),
                               month=regex_result_group.group('month'),
                               day=regex_result_group.group('day'))
    else:
        return {
            'comercio_id': regex_result_group.group('comercio_id'),
            'etl_run': regex_result_group.group('run_time'),
            'timestamp': '{y}/{m}/{d}T{h}-{M}-{s}'
            ''.format(y=regex_result_group.group('year'),
                      m=regex_result_group.group('month'),
                      d=regex_result_group.group('day'),
                      h=regex_result_group.group('hours'),
                      M=regex_result_group.group('mins'),
                      s=regex_result_group.group('seconds')),
            'bucket': 'comercio-sepa-{id}'
            ''.format(id=regex_result_group.group('comercio_id'))
            }


@profile
def dowload_s3_data(remote_file,
                    bucket=DEFAULT_BUCKET,
                    dst_folder=FOLDER,
                    s3=S3,
                    delete_original=False):
    global all_download_threads
    global actual_download_thread
    global all_download_threads_ok
    all_download_threads += 1
    actual_download_thread += 1

    bucket = s3.Bucket(bucket)
    try:
        local_file = os.path.join(dst_folder, remote_file)
        bucket.download_file(remote_file, local_file)
        if delete_original:
            s3.Object(bucket, remote_file).delete()
        all_download_threads_ok += 1
        actual_download_thread -= 1
        return True
    except Exception as e:
        print e
        actual_download_thread -= 1
        return False


@profile
def build_file_list(bucket=DEFAULT_BUCKET,
                    selected_bucket=SELECTED_BUCKET,
                    name_part='sepa_'):
    all_reg = 0
    glacier_regs = 0
    not_glacier_regs = 0
    to_print.append(['filename',
                     'comercio_id',
                     'etl_run',
                     'storage_class',
                     'file_type',
                     'Downloadable',
                     'upload_timestamp'])
    to_print_glacier = to_print
    for obj in selected_bucket.objects.all():
        r = re.search(VALID_FILNAME_REGEX, obj.key)
        newkey = have_all_fields_required(r, FILENAME_PARTS)
        if newkey not in white_list and type(newkey) is str:
            white_list.append(newkey)
            upload_timestamp = '{y}/{m}/{d} {h}:{mm}:{s}'
            ''.format(y=r.group('year'),
                      m=r.group('month'),
                      d=r.group('day'),
                      h=r.group('hours'),
                      mm=r.group('mins'),
                      s=r.group('seconds'))

            if obj.storage_class == 'GLACIER':
                glacier_regs = glacier_regs + 1
            else:
                not_glacier_regs = not_glacier_regs + 1
            to_print.append([obj.key,
                             r.group('comercio_id'),
                             '06:00' if r.group('run_time') == 1 else '10:00',
                             obj.storage_class,
                             r.group('format'),
                             'V' if obj.storage_class != 'GLACIER' else 'F',
                             upload_timestamp])
            to_download.append({
                'filename': obj.key,
                'com_id': r.group('comercio_id'),
                'run_time': '06:00' if r.group('run_time') == 1 else '10:00',
                'storage_class': obj.storage_class,
                'format': r.group('format'),
                'downloadable': 'V' if obj.storage_class != 'GLACIER' else 'F',
                'timestamp': upload_timestamp
            })
        all_reg = all_reg + 1

    dispatch_threads_download(to_print, headers=True)
    tablec = []
    tablec.append(', '.join(to_print[0]))
    for row in sorted(to_print[1:len(to_print)], key=lambda tup: tup[3]):
        if not type(row) is None:
            tablec.append(', '.join(row))

    print len(tablec)
    print '\n'.join(tablec)

    return {
        'data': r,
        'all_regs': all_reg,
        'glacier_regs': glacier_regs,
        'not_glacier_regs': not_glacier_regs
        }


@profile
def get_files_list(bucket=DEFAULT_BUCKET,
                   selected_bucket=SELECTED_BUCKET,
                   storage_class=DEFAULT_STORAGE_CLASS,
                   name_part=''):
    if not bucket == DEFAULT_BUCKET:
        s3 = boto3.resource('s3')
        selected_bucket = S3.Bucket(bucket)
    storage_class = storage_class.upper()
    for obj in selected_bucket.objects.all():
        if obj.storage_class == storage_class and name_part in obj.key:
            print '| {filename}\t| {st_clss}\t|'.format(filename=obj.key,
                                                        st_clss=storage_class)


def main():
    if not os.path.exists(FOLDER):
        shutil.rmtree(FOLDER, ignore_errors=True)
    bulk = build_file_list()
    global all_download_threads
    global all_download_threads_ok
    global actual_download_thread
    while actual_download_thread > 0:
        time.sleep(1)
    print bulk['data']
    print '\nResultados: '
    print '============='
    print '\t + Bucket: \"comercio-sepa-glacier\"'
    print '\t + Archivo: \"sepa.zip\"'
    print '\t   + Disponibles SC=[STANDARD]', bulk['not_glacier_regs']
    print '\t   + Disponibles SC=[GLACIER]', bulk['glacier_regs']
    print '\t   + \"sepa.zip\" Descargados: ', all_download_threads_ok
    print '\t   + Thread \"sepa.zip\" lanzados: ', all_download_threads
    print '(Total de archivos en Bucket: %s )' % str(bulk['all_regs'])
    print_prof_data()

if __name__ == '__main__':
    main()
