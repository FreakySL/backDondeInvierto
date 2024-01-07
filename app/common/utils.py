from datetime import (
    datetime,
    date,
)
from decimal import (
    Decimal,
    InvalidOperation,
    localcontext,
    ROUND_DOWN,
)
import logging
from pytz import timezone as pytz_timezone

from .constants import (
    DECIMAL_ZERO,
    TIME_ZONE,
    DECIMAL_DIGIT_AMOUNT,
)
from .exceptions import ParameterError


def get_logger(name):
    """Get and return logger with given name."""
    if not name:
        raise Exception("Need to provide a name for the logger")
    return logging.getLogger(name)


logger = get_logger(__name__)


def datetime_to_utc(date, with_tzinfo=False):
    """
    Return date in UTC datetime.

    By default tzinfo is removed, unless `with_tzinfo` is True.
    """
    ret = date
    if date.tzinfo:
        ret = date.astimezone(pytz_timezone.gettz('UTC'))
        if not with_tzinfo:
            ret = ret.replace(tzinfo=None)
    return ret


def get_current_time(timezone_str=None):
    """
    Get and return the current time of a given place.

    :param timezone_str: The timezone string for the location, defaulted to
        the one of Buenos Aires, Argentina.
    :return: The datetime object with the current time for the given place.
    """
    # In case we eventually want to fetch the timezone string based on the
    # location name:
    #   g = geocoders.GoogleV3()
    #   tz = tzwhere.tzwhere()
    #   place, (lat, lng) = g.geocode(location_name)
    #   tz_str = tz.tzNameAt(lat, lng)
    if timezone_str is None:
        timezone_str = TIME_ZONE

    tz_obj = pytz_timezone(timezone_str)
    now_time = datetime.now(tz_obj)
    return now_time


def get_decimal_value(_str):
    """
    Transforms a string with format '99,99' into Decimal(99.99)
    or returns None if exception.
    """
    value = None
    try:
        value = Decimal(str(_str.replace(",", ".")))
    except InvalidOperation:
        logger.warn("Could not transform '%s' into Decimal", _str)
        value = None

    return value


def normalize_amount(value):
    """
    Normalice value on amount format.

    Having same number of decimals.
    """
    number_of_decimals = None
    if value is None:
        value = DECIMAL_ZERO

    number_of_decimals = DECIMAL_DIGIT_AMOUNT
    normal_value = normalize_decimals(value, number_of_decimals)
    return normal_value


def normalize_decimals(value, decimals_to_round, method=ROUND_DOWN):
    """
    Normalize decimal value to the decimals given.

    @param value: anything castable to Decimal, original value
    @param decimals_to_round: Integer, number of decimals to have
    @param method: str (?), round method to use. not used so far.
    """
    new_value = value

    if value is None:
        return DECIMAL_ZERO

    if not isinstance(value, str) and not isinstance(value, Decimal):
        value = str(value)

    if not isinstance(value, Decimal):
        new_value = Decimal(value)

    decimals = Decimal(f'0.{"0" * decimals_to_round}')
    with localcontext() as ctx:
        ctx.rounding = method
        new_value = new_value.quantize(decimals)
    return new_value


def parse_date(date_str):
    """
    Parse a string with format DD/MM/YYYY or YYYY/MM/DD to a date object.
    """
    # Cambiamos los "/" por "-"
    date_str = date_str.replace("/", "-")

    # Parseamos el string dependiendo del formato
    try:
        date_obj = datetime.strptime(date_str, "%d-%m-%Y").date()
    except ValueError:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()

    return date_obj


def proportion_of(a, b, decimals=2):
    """ Calculate the difference between two number and return the percentage"""
    # Catching  the common exception of divisions
    if a is None or b in [0, None]:
        percentage = DECIMAL_ZERO
    else:
        percentage = ((a / b) - 1) * 100

    percentage = normalize_decimals(percentage, 2)

    return percentage


def get_ip_from_request(request):
    x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
    if x_forwarded_for:
        ip = x_forwarded_for.split(',')[0]
    else:
        ip = request.META.get('REMOTE_ADDR')
    return ip


def date_or_today(__date=None):
    """
    Return a date for use in batch script as datetime.date instance
    Return current day if date is None.
    """
    if __date is None:
        __date = get_current_time().date()
    elif isinstance(__date, datetime):
        # expected date in datetime format
        __date = __date.date()
    elif isinstance(__date, date):
        # date is already a date elem
        pass
    elif isinstance(__date, str):
        # expected date in iso format YYYY-MM-DD
        try:
            __date = datetime.fromisoformat(__date).date()
        except ValueError as e:
            err_msg = "Date tiene un formato raro: %s. %s." % (__date, e)
            logger.error(err_msg)
            raise ParameterError(err_msg)
    else:
        err_msg = "La fecha es del tipo incorrecto: %s." % type(__date)
        logger.error(err_msg)
        raise ParameterError(err_msg)
    return __date
