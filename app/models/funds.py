import math
import time
from datetime import timedelta
import json
import requests
from requests.exceptions import ConnectionError
from decimal import Decimal
from multiprocessing import (
    Pool,
)

from ..common.utils import (
    get_logger,
    get_current_time,
    normalize_decimals,
    get_last_friday,
)


logger = get_logger(__name__)


RISK_LEVEL_DIC = {
    "Corto Plazo": 0,
    "Mediano Plazo": 1,
    "Largo Plazo": 2,
    "Flexible": 2,
}

RESCUE_TIME_DIC = {
    "4": 0,  # Mercado de Dinero
    "3": 24,  # Renta Fija
    "2": 48,  # Renta Variable
    "5": 48,  # Renta Mixta
    "7": 48,  # Retorno Total
    "6": 48,  # Pymes
    "8": 48,  # Infraestructura
    "Otras": 48,
}

MAX_RETRIES = 5  # Make this a configurable parameter


class FundClassParser():
    """Clase de una Serie de Fondo, matcheado con un asset.
    sheet example:
    | class | name | trading_currency | class_cafci_code | fund_cafci_code | rescue_time | risk_level | tne | monthly_performance | updated | logo_url
    """
    SHEET = "funds"
    COLUMN_MAX_RANGE = "A1:K"
    BASE_CAFCI_URL = "https://api.cafci.org.ar"
    FUND_CODES_CELL_RANGE = "D2:E"
    TEM_MONTHLY_UPDATED_RANGE = "H2:J"

    # Create the init
    def __init__(self,):
        pass

    def get_cafci_ficha_default(self):
        response = requests.get(
            "https://api.cafci.org.ar/fondo/1222/clase/3924/ficha",
        )

        return response.status_code == 200

    def get_tem(self, initial_price: Decimal, final_price: Decimal) -> Decimal:
        """
        Calculate TEM from data.
        param: initial_price: Decimal
        param: final_price: Decimal
        return: TEM: Decimal
        """
        if initial_price is None or final_price is None:
            return None

        tem = ((final_price - initial_price) / initial_price) * 100 * Decimal("4.28")  # 4.28 is the number of periods in a month

        tem = normalize_decimals(tem, 2)
        # Return the TEM
        return tem

    def get_max_range(self):
        return self.COLUMN_MAX_RANGE

    def get_sheet(self):
        return self.SHEET

    def get_fund_codes_range(self):
        return self.FUND_CODES_CELL_RANGE

    def get_tem_monthly_updated_range(self):
        return self.TEM_MONTHLY_UPDATED_RANGE

    def get_monthly_performance(self):
        #
        pass

    def get_index(self):
        return "class_cafci_code"

    def validated_cafci_response(self, response):
        if response.status_code != 200:
            logger.error("Error getting cafci response: %s", response.text)
            raise Exception("Error getting cafci response: %s", response.text)

        return True

    def get_all_fund_groups(self):
        cafci_funds_url = self.BASE_CAFCI_URL + "/fondo?estado=1&include=gerente,tipoRenta,clase_fondo&limit=0"
        logger.info("Getting all funds from cafci")
        response = requests.get(cafci_funds_url)

        self.validated_cafci_response(response)

        parsed_response = response.json()["data"]
        logger.info("Got %s funds from cafci", len(parsed_response))

        return parsed_response

    def get_fund_classes_by_fund_group(self, fund_group_data: dict):
        fund_classes = []
        fund_id = fund_group_data.get('id')
        rescue_time = fund_group_data.get('tipoRenta').get('id')  # "3"
        # Format the rescue time to obtain the rescue time id, example: "3" -> 24
        rescue_time = RESCUE_TIME_DIC.get(rescue_time)  # 24

        risk_level = fund_group_data.get('horizonteViejo')  # "Corto Plazo"
        # Format the rescue time to obtain the rescue time id, example: "Corto Plazo" -> 0
        risk_level = RISK_LEVEL_DIC.get(risk_level)  # 0

        trading_currency = fund_group_data.get("monedaId")  # "1"
        # Format the trading currency to obtain the currency name, example: 1 -> ARS | 2 -> USD
        trading_currency = "ARS" if trading_currency == "1" else "USD"

        if trading_currency == "USD" and rescue_time == 0:
            rescue_time = 24

        name = fund_group_data.get("nombre")
        updated = get_current_time().strftime("%d-%m-%Y")

        for fund in fund_group_data.get("clase_fondos"):
            class_id = fund.get('id')
            class_name = fund.get('nombre')  # ST Zero - Clase D
            # Format the name to obtain the class name, example: D
            class_name_formated = class_name.split(" ")[-1]  # D

            if len(class_name_formated) > 1:
                class_name_formated = None

            logger.info(f'Creando data de fondo/codigo: {class_id}/{name}')

            # Crear data de fondo ejemplo: [class_name, fund_name, trading_currency, class_cafci_code, fund_cafci_code, rescue_time, risk_level, tem, monthly_performance, updated, logo_url]
            fund_class_data = [class_name_formated, name, trading_currency, class_id, fund_id, rescue_time, risk_level, None, None, updated, None]

            fund_classes.append(fund_class_data)

        return fund_classes

    def get_all_funds(self):
        response = self.get_all_fund_groups()
        all_fund_classes = []

        for fund_group in response:
            fund_classes = self.get_fund_classes_by_fund_group(fund_group)
            all_fund_classes.extend(fund_classes)

        return all_fund_classes

    def get_seven_days_price(self, class_id, fund_id):
        """
        Get the first and last price of the last seven days.
        """
        cafci_performance_url = f"{self.BASE_CAFCI_URL}/fondo/{fund_id}/clase/{class_id}/rendimiento/"

        today = get_last_friday()
        one_week_ago = today - timedelta(days=7)

        params = f"{one_week_ago.strftime('%Y-%m-%d')}/{today.strftime('%Y-%m-%d')}"
        cafci_performance_url = cafci_performance_url + params

        response = None
        logger.info("Getting cafci performance from %s", cafci_performance_url)
        for i in range(MAX_RETRIES):
            try:
                cafci_response = requests.get(cafci_performance_url)
                response = cafci_response.json()
                break

            except ConnectionError as e:
                wait_time = 60 * i  # Exponential backoff
                logger.warning(f"ConnectionError: {e}. Retrying in {wait_time} seconds.")
                time.sleep(wait_time)

            except Exception as e:
                logger.error("Error getting cafci performance: %s - status code: %s", e, cafci_response.status_code)
                return None, None

        if response is None:
            logger.error(f"Error getting cafci performance after {MAX_RETRIES} retries")
            return None, None

        has_errors = response.get('error')  # Possible errors are 'wrong-dates' and 'inexistence'
        if has_errors:
            logger.debug(f"Wrong dates for {class_id}/{fund_id} in cafci")
            return None, None

        returned_elems = response.get('data')

        # Normalize the shares to avoid problems with the decimal field
        first_price = normalize_decimals(returned_elems.get('desde').get('valor')) / 1000
        last_price = normalize_decimals(returned_elems.get('hasta').get('valor')) / 1000

        return first_price, last_price

    def get_last_monthly_performance(self, class_id, fund_id):
        """
        Get the last monthly performance.
        """
        cafci_performance_url = f"{self.BASE_CAFCI_URL}/fondo/{fund_id}/clase/{class_id}/rendimiento/"

        today = get_last_friday()
        one_month_ago = today - timedelta(days=30)

        params = f"{one_month_ago.strftime('%Y-%m-%d')}/{today.strftime('%Y-%m-%d')}"
        cafci_performance_url = cafci_performance_url + params

        logger.info("Getting cafci performance from %s", cafci_performance_url)

        response = None

        for i in range(MAX_RETRIES):
            try:

                cafci_response = requests.get(cafci_performance_url)
                response = cafci_response.json()
                break

            except ConnectionError as e:
                wait_time = 60 * i  # Exponential backoff
                logger.warning(f"ConnectionError: {e}. Retrying in {wait_time} seconds.")
                time.sleep(wait_time)

            except Exception as e:
                logger.error("Error getting cafci performance: %s", e)
                break

        if response is None:
            logger.error(f"Error getting cafci performance after {MAX_RETRIES} retries")
            return None

        has_errors = response.get('error')  # Possible errors are 'wrong-dates' and 'inexistence'

        if has_errors:
            logger.debug(f"Wrong dates for {class_id}/{fund_id} in cafci")
            return None

        returned_elems = response.get('data')

        # Normalize the shares to avoid problems with the decimal field
        monthly_performance = normalize_decimals(returned_elems.get('rendimiento'), 2)

        return monthly_performance
