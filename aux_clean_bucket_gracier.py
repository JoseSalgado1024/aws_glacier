#!/usr/bin/python
import boto3
import os
from boto3 import client
from tabulate import tabulate

GLACIER='comercio-sepa-glacier'
FOLDER='bucket_glacier'
PREFIX='sepa_'
aws_conn = client('s3')
white_list=[]
to_download=[]
all_list=[]



def dowload_s3_data(bucket, remote_file, dst_folder,
                    s3=boto3.resource('s3'),
                    delete_original=False):
    if not os.path.exists(dst_folder):
        os.makedirs(dst_folder)
    bucket = s3.Bucket(bucket)
    try:
        local_file = os.path.join(dst_folder, remote_file)
        bucket.download_file(remote_file, local_file)
        if delete_original:
            s3.Object(bucket, remote_file).delete()
        return True
    except Exception:
        print 's3://{bucket}/{filename} is in GLACIER now'.format(bucket=GLACIER,
                                                                  filename=remote_file)
        return False

def main():
    for sepa_zip in aws_conn.list_objects(Bucket=GLACIER,
                                          Prefix=PREFIX,
                                          MaxKeys=100000)['Contents']:
        if sepa_zip['Key'][:-13] not in white_list:
            white_list.append(sepa_zip['Key'][:-13])
            to_download.append(sepa_zip['Key'])
        all_list.append(sepa_zip['Key'])
    p = 0
    for sepa_zip in to_download:
        s = dowload_s3_data(GLACIER, sepa_zip, FOLDER)
        # print '{porcentaje}% \t{filename}\t{status}'.format(porcentaje=int((p*100)/len(to_download)), filename=sepa_zip, status=s)
        p = p + 1

    #print '\n'.join(to_download)
    print 'Registros en WL(Registros Unicos): ',len(white_list)
    print 'Registros en total: ',len(all_list)


if __name__ == '__main__':
    main()
