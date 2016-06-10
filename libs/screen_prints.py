#!/usr/bin/python
# encoding: utf-8


LANGUAGES = ['ES', 'EN']

SCREEN_PRINTS = [
    # AWS
    {'INVALID_CREDENTIALS': ['Las credenciales de AWS provistas no son validas.', 'The AWS credentials are invalid.']},
    {'INVALID_ID_OR_SECRET_KEY': ['La clave ID de acceso[{aki}] o la clave secreta[{sak}] de AWS no es valida.', 'The AWS access key ID[{aki}] o secret access[{sak}] are invalid.']},
    {'INVALID_REGION': ['El nombre de region AWS, \"{r}\" no es valido.', 'Region name, \"{r}\", is invalid.']},
    {'BUCKET_NOT_EXISTS': ['El Bucket: \"{bucket}\" no extiste!.', 'Bucket: \"{bucket}\" doesn\'t exists!.']},
    #Files
    {'FILE_NOT_EXISTS': ['El archivo \"{f}\" no existe!.', 'File \"{f}\" doesn\'t exists!.']},
    {'FILE_NOT_NEED_BE_RESTORED': ['El archivo \"{f}\" esta: {s}', 'File \"{f}\" it\'s {s}.']},
    {'FILE_NOT_AVAILABLE': ['El archivo \"{f}\" no esta disponible.', 'File \"{f}\" isn\'t available.']},
    {'FILE_READY_FOR_DOWNLOAD': ['El archivo \"{f}\" esta disponible y listo para ser descargado.', 'File \"{f}\" is available and ready for download it.']},
    {'FILE_EXISTS_BUT_NOT_AVAILABLE': ['El archivo \"{f}\" exite, pero no esta disponible.', 'File \"{f}\" exists, but isn\'t available.']},
    {'FILE_EXISTS_BUT_NOT_AVAILABLE_GLACIER': ['El archivo \"{f}\" exite, pero no es posible descargalo debido a que esta alamacenado bajo la clase \"GLACIER\".', 'File \"{f}\" exists, but can\'t download it,  because it\'s stored under \"GLACIER\" class.']},
    {'FILE_STORAGE_CLASS_IS_NOT_GLACIER': ['La \"storage_class\" del archivo: \"{f}\" no es \"GLACIER\"', 'Storage_Class to \"{f}\" isn\'t \"GLACIER\"']},
    {'CANT_RESTORE_FILE': ['Es imposible restaurar el archivo: \"{f}\"', 'Imposible restore file: \"{f}\"']},
    {'BAD_FILENAME': ['Nombre de mal formado o invalido[{f}]', 'Filename bad formatted or invalid[{f}]']},


    # Loading
    {'LOADING_CONF': ['Cargando archivo de configuracion: \"{f}\"...', 'Loading config file: \"{f}\"...']},
    {'LOADING_FILE': ['Cargando archivo: \"{f}\"...', 'Loading file: \"{f}\"...']},
    {'FILE_LOAD_OK': ['El archivo: \"{f}\" se cargo correctamente.', 'File \"{f}\" successful loaded,']},
    {'CONF_FILE_LOAD_FAIL': ['No se pudo cargar el archivo de configuración: \"{f}\".', 'Can\'t load config file: \"{f}\".']},
    {'FILE_LOAD_FAIL': ['No se pudo cargar el archivo: \"{f}\".', 'Can\'t load file: \"{f}\".']},

    #Common Errors
    {'FATAL_ERROR_ABORTING': ['ERROR FATAL!, se abortará y terminará el proceso!', 'FATAL ERROR!, abort and exit!']},
    {'UNKNOW_ERROR': ['Error desconocido...', 'Unknow error...']}
]

class BadMsgs(Exception):
    def __init__(self):
        super(BadMsgs, self).__init__('No están disponibles todos los textos en todos los lenguajes.')

class LanguageNotExists(Exception):
    def __init__(self):
        super(LanguageNotExists, self).__init__('No existe el lenguaje seleccionado.')

class LanguageLoadFail(Exception):
    def __init__(self):
        super(LanguageLoadFail, self).__init__('Fallo en carga de mensajes..')


class Messages(object):
    def __init__(self,
                 msg_list=None,
                 available_languages=None,
                 selected_language='ES'):
        for m in msg_list:
            for k,v in m.items():
                if len(v) != len(available_languages):
                    raise BadMsgs

        if selected_language not in available_languages:
            raise LanguageNotExists
        lang_index = available_languages.index(selected_language)
        for m in msg_list:
            k, v = m.items()[0]
            setattr(Messages, k, v[lang_index])


try:
    mensages = Messages(SCREEN_PRINTS, LANGUAGES, 'EN')
except Exception as e:
    print e
