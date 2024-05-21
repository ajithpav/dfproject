from datetime import datetime, timedelta
from pytz import timezone
import os, hashlib, uuid, operator
from decimal import Decimal
import logging, json
import locale
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
logging.basicConfig(level=logging.INFO)


def no_format(value):
    _value = float(value)
    return f'{_value:n}'

def comma_format(value):
    _value = float(value)
    return "{:,.2f}".format(_value)

def formatINR(number):
    _value = float(number)
    _number = "{:.2f}".format(_value)
    s, *d = str(_number).partition(".")
    r = ",".join([s[x-2:x] for x in range(-3, -len(s), -2)][::-1] + [s[-3:]])
    # return "".join([r] + d)
    return r

# ************************************ datetime functions start ************************************
FORMAT = '%Y-%m-%d %H:%M:%S'
ops = { '-': operator.sub, '+': operator.add }

def current_datetime():
    '''return current ist datetime with format: %Y-%m-%d %H:%M:%S'''
    return datetime.now().astimezone(timezone('Asia/Kolkata')).strftime(FORMAT)    

def dt_parse(dt):
    return datetime.strptime(dt, FORMAT) if type(dt) is str else dt

def addorsub_seconds(dt, sec, op='-'):
    '''return datetime by adding or subtracting given seconds from the given datetime string'''
    if type(dt) == datetime: dt = dt.strftime(FORMAT)
    return ops[op](dt_parse(dt), timedelta(seconds=sec)).strftime(FORMAT)

def get_seconds_from_two_datetimes(dt1, dt2):
    '''return seconds (difference of two datetimes)'''
    return int( (dt_parse(dt1) - dt_parse(dt2)).total_seconds() )

def get_diff_month(date1, date2):
    return (date1.year - date2.year) * 12 + date1.month - date2.month

# ************************************ datetime functions end ************************************

def generate_16randomchar():
    random_data = os.urandom(128)
    _id = hashlib.md5(random_data).hexdigest()[:16]
    return str(_id)


def generate_id():
    return str(uuid.uuid4().hex)


def get_list_size(data, val=None):
    return val if data is None else int(len(data))


def check_dict(data, key, val=None):
    return data[key] if data and data[key] else val


def split_array(source, count):
    return [source[i:i + count] for i in range(0, len(source), count)]


def float_to_decimal(val):
    return Decimal(str(val))
