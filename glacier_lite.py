from libs.glacier import Glacier
from libs.configs import  my_conf

"""

"""




def main():
    print 'VAULT_NAME: ', my_conf.VAULT_NAME
    print 'KEY_ID: ', my_conf.ACCESS_KEY_ID
    print 'ACCESS_KEY: ', my_conf.SECRET_ACCESS_KEY
    print 'REGION_NAME: ', my_conf.REGION
    galcier = Glacier(my_conf.VAULT_NAME,
                      my_conf.ACCESS_KEY_ID,
                      my_conf.SECRET_ACCESS_KEY,
                      my_conf.REGION)
    galcier.retrieve('sepa_1_comercio-sepa-10_2016-04-22_03-46-32.zip', True)




if __name__ == '__main__':
    main()
