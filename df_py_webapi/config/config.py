from os import environ
from dotenv import load_dotenv
import logging
load_dotenv('.env')
logging.basicConfig(format='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s', level=logging.INFO)

config = {
    "URL": {
        "BASE_URL": environ.get('BASE_URL')
    },
    "DB": {
        "MARIA_DB": environ.get('MARIA_DB')
    },
    "USR_PWD": {
        "USER_NAME": environ.get('USER_NAME'),
        "PASSWORD": environ.get('PASSWORD')
    },
    "SECURITY" : {
        "KEY" : environ.get('COMMON_ENCRYPTION_KEY'),
        "IV" : environ.get('COMMON_16_BYTE_IV_FOR_AES')
    },
    "AZURE_KEYS" : {
        "CLIENT_ID" : environ.get('CLIENT_ID'),
        "TENANT_ID" : environ.get('TENANT_ID'),
        "CLIENT_SECRET" : environ.get('CLIENT_SECRET'),
    }
}
