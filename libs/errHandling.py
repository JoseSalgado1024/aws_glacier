class CustomExcept(Exception):
    def __init__(self, message='Ups! something going wrong...', errors=None):
        super(CustomExcept, self).__init__(message)
        self.errors = errors

class BadAuthData(Exception):
    def __init__(self, aki=None, sak=None, rn=None):
        super(BadAuthData, self).__init__('\nLas credenciales (AWS) provistas, no son validas...\n'\
                                          'Chequear:\n'\
                                          '+ access_key_id=\"{aki}\"\n'\
                                          '+ secret_access_key=\"{sak}\"\n'\
                                          '+ region_name=\"{rn}\"\n'.format(rn=rn,
                                                                        aki=aki,
                                                                        sak=sak))

class BadFileOrBucket(Exception):
    def __init__(self, bucket_name=None, name_regex=None):
        super(BadFileOrBucket, self).__init__('\n\"BUCKET_NAME\" o \"NAME_REGEX" es invalido \n'\
                                              'Chequear:\n'\
                                              '+ name_regex={nr}\n'\
                                              '+ bucket={bn}\n'.format(bn=bucket_name,
                                                                       nr=name_regex))

class BadRestoreDays(Exception):
    def __init__(self):
        super(BadRestoreDays, self).__init__('\n\"RESTORE_FOR\" es invalido\n'\
                                             'Chequear:\n'\
                                             '+ restore_for=XX\n')

class BucketNotExists(Exception):
    def __init__(self):
        super(BucketNotExists, self).__init__('\n\"BUCKET_NAME\" es invalido\n'\
                                              'Chequear:\n'\
                                              '+ bucket=Xxxx-Xxxx\n')

class BadFileListToRestore(Exception):
    def __init__(self):
        super(BadFileListToRestore, self).__init__('\n\"LIST_OF_FILES\" es invalido\n'\
                                                   'Chequear:\n'\
                                                   '+ LIST_OF_FILES=[\'f0.ext\',\'f1.ext\',\'..\',\'fn.ext\']\n')
