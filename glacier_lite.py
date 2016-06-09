from libs.glacier import Glacier
from libs.configs import  my_conf

"""
    def __init__(self,
                access_key_id=None,
                secret_access_key=None,
                region_name='us-east-1',
                bucket=None,
                filename=None,
                restore_for=5):
"""




def main():
    try:
        glacier = Glacier(bucket=my_conf.BUCKET_NAME,
                          access_key_id=my_conf.ACCESS_KEY_ID,
                          secret_access_key=my_conf.SECRET_ACCESS_KEY,
                          region_name=my_conf.REGION,
                          name_regex=my_conf.NAME_REGEX)
        glacier.restore_list_of_files(['sepa_2_comercio-sepa-9_2016-05-05_01-01-05.zip'])
    except Exception as e:
        print e

    # galcier.retrieve('sepa_1_comercio-sepa-10_2016-04-22_03-46-32.zip', True)




if __name__ == '__main__':
    main()
