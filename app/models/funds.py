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

RISK_LEVEL_DICT = {
    "4": 0,  # Mercado de Dinero
    "3": 1,  # Renta Fija
    "2": 2,  # Renta Variable
    "5": 2,  # Renta Mixta
    "7": 1,  # Retorno Total
    "6": 2,  # Pymes
    "8": 2,  # Infraestructura
    "Otros": 2,
}

MAX_RETRIES = 5  # Make this a configurable parameter


class FundClassParser():
    """Clase de una Serie de Fondo, matcheado con un asset.
    sheet example:
    | class | name | trading_currency | class_cafci_code | fund_cafci_code | rescue_time | risk_level | tna | tea | tem | monthly_performance | six_months_performance | year_performance | updated | logo_url |
    """
    SHEET = "funds"
    COLUMN_MAX_RANGE = "A2:N"
    BASE_CAFCI_URL = "https://api.cafci.org.ar"
    FUND_CODES_CELL_RANGE = "D2:E"
    CALC_DATE_RANGE = "H2:N"
    START_COLUMN = "A"
    END_COLUMN = "N"
    TNA_COLUMN = "H"

    # Create the init
    def __init__(self,):
        pass

    def perform_request(self, url, method="GET", data=None, headers=None, params=None, json_data=None):
        response = None
        for i in range(MAX_RETRIES):
            try:
                response = requests.request(
                    method=method,
                    url=url,
                    data=data,
                    headers=headers,
                    params=params,
                    json=json_data,
                )
                response = response.json()
                break

            except ConnectionError as e:
                wait_time = 60 * i
                logger.warning(f"ConnectionError: {e}. Retrying in {wait_time} seconds.")
                time.sleep(wait_time)

            except Exception as e:
                logger.error("Error getting response: %s", e)
                return None

        if response is None:
            logger.error(f"Error getting response after {MAX_RETRIES} retries")
            return None

        return response

    def get_cafci_ficha_default(self):
        response = requests.get(
            "https://api.cafci.org.ar/fondo/1222/clase/3924/ficha",
        )

        return response.status_code == 200

    def get_tem(self, initial_price: Decimal, final_price: Decimal, interval=7) -> Decimal:
        """
        Calculate TEM from data.
        param: initial_price: Decimal
        param: final_price: Decimal
        return: TEM: Decimal
        """
        if initial_price is None or final_price is None:
            return None

        tem = ((final_price / initial_price) ** (Decimal(30) / Decimal(interval)) - 1) * 100
        tem = normalize_decimals(tem, 2)

        # Return the TEM
        return tem

    def get_tea(self, initial_price: Decimal, final_price: Decimal, interval=7) -> Decimal:
        """
        Calculate TEA from data.
        param: initial_price: Decimal
        param: final_price: Decimal
        return: TEA: Decimal
        """
        if initial_price is None or final_price is None:
            return None

        tea = ((final_price / initial_price) ** (Decimal(365) / Decimal(interval)) - 1) * 100
        tea = normalize_decimals(tea, 2)
        # Return the TEA
        return tea

    def get_tna(self, initial_price: Decimal, final_price: Decimal, interval=7) -> Decimal:
        """
        Calculate TNA from data.
        param: initial_price: Decimal
        param: final_price: Decimal
        return: TNA: Decimal
        """
        if initial_price is None or final_price is None:
            return None

        tna = ((final_price / initial_price) - 1) * 100 * (Decimal(365) / Decimal(interval))

        tna = normalize_decimals(tna, 2)
        # Return the TNA
        return tna

    def get_proyection(self, initial_price: Decimal, final_price: Decimal, interval=7) -> list:
        """
        Calculate proyection from data.
        param: initial_price: Decimal
        param: final_price: Decimal
        return: [tem, tna, tea]: list
        """
        tem = self.get_tem(initial_price, final_price, interval)
        tna = self.get_tna(initial_price, final_price, interval)
        tea = self.get_tea(initial_price, final_price, interval)

        # Return the proyection
        return [tem, tna, tea]

    def get_max_range(self):
        return self.COLUMN_MAX_RANGE

    def get_sheet(self):
        return self.SHEET

    def get_fund_codes_range(self):
        return self.FUND_CODES_CELL_RANGE

    def get_calc_data_range(self):
        return self.CALC_DATE_RANGE

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
        response = self.perform_request(url=cafci_funds_url)

        self.validated_cafci_response(response)

        parsed_response = response.get("data")
        logger.info("Got %s funds from cafci", len(parsed_response))

        return parsed_response

    def get_fund_classes_by_fund_group(self, fund_group_data: dict):
        fund_classes = []
        fund_id = fund_group_data.get('id')
        rescue_time = int(fund_group_data.get('diasLiquidacion')) * 24  # 3 * 24 = 72
        if rescue_time > 72:
            rescue_time = 72

        risk_level = fund_group_data.get('tipoRenta').get('id', "Otros")  # "1"
        # Format the rescue time to obtain the rescue time id, example: "Corto Plazo" -> 0
        risk_level = RISK_LEVEL_DICT.get(risk_level, 2)  # 0

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

            if class_name_formated != "A":
                continue

            logger.info(f'Creando data de fondo/codigo: {class_id}/{name}')

            # Crear data de fondo ejemplo: [class_name, fund_name, trading_currency, class_cafci_code, fund_cafci_code, rescue_time, risk_level, tna, tea, tem, monthly_performance, year_performance, updated, logo_url]
            fund_class_data = [class_name_formated, name, trading_currency, class_id,
                               fund_id, rescue_time, risk_level, None, None, None, None, None, updated, None]

            fund_classes.append(fund_class_data)

        return fund_classes

    def get_all_funds(self):
        response = self.get_all_fund_groups()
        all_fund_classes = []

        for fund_group in response:
            fund_classes = self.get_fund_classes_by_fund_group(fund_group)
            all_fund_classes.extend(fund_classes)

        return all_fund_classes

    def get_prices_by_range(self, class_id: str, fund_id: str, date_range: int) -> list:
        """
        Get the first and last price of the last seven days.
        param: class_id - Fund class id
        param: fund_id - Fund id
        param: date_range - Date range in days
        return: first_price, last_price - Prices in pesos
        """
        cafci_performance_url = f"{self.BASE_CAFCI_URL}/fondo/{fund_id}/clase/{class_id}/rendimiento/"

        today = get_last_friday()
        start_date = today - timedelta(days=date_range)

        params = f"{start_date.strftime('%Y-%m-%d')}/{today.strftime('%Y-%m-%d')}"
        cafci_performance_url = cafci_performance_url + params

        logger.info("Getting cafci performance from %s", cafci_performance_url)
        response = self.perform_request(url=cafci_performance_url)

        if not response:
            logger.error(f"Error getting cafci performance after {MAX_RETRIES} retries")
            return None, None

        has_errors = response.get('error')  # Possible errors are 'wrong-dates' and 'inexistence'
        if has_errors:
            logger.warning(f"Wrong dates for {fund_id}/{class_id} in cafci")
            return 0, 0

        returned_elems = response.get('data')

        # Normalize the shares to avoid problems with the decimal field
        first_price = normalize_decimals(returned_elems.get('desde').get('valor')) / 1000
        last_price = normalize_decimals(returned_elems.get('hasta').get('valor')) / 1000

        return first_price, last_price

    def get_performance_by_range(self, class_id: str, fund_id: str, date_range: int) -> Decimal:
        """
        Get the last monthly performance.
        param: class_id - Fund class id
        param: fund_id - Fund id
        param: date_range - Date range in days
        return: performance - Performance in percentage
        """

        today = get_last_friday()
        start_date = today - timedelta(days=date_range)
        end_date = today

        cafci_performance_url = f"{self.BASE_CAFCI_URL}/fondo/{fund_id}/clase/{class_id}/rendimiento/"
        params = f"{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}"
        cafci_performance_url = cafci_performance_url + params
        logger.info("Getting cafci performance from %s", cafci_performance_url)

        response = self.perform_request(url=cafci_performance_url)

        if not response:
            logger.error(f"Error getting cafci performance after {MAX_RETRIES} retries")
            return None

        has_errors = response.get('error')

        if has_errors:
            logger.debug(f"{has_errors} for {class_id}/{fund_id} in cafci")
            return 0

        returned_elems = response.get('data')

        # Normalize the shares to avoid problems with the decimal field
        performance = normalize_decimals(returned_elems.get('rendimiento'), 2)

        return performance

    def get_fund_class_by_class_and_fund(self, class_id, fund_id):
        """
        Get the fund class by class id and fund id.
        """
        cafci_funds_url = self.BASE_CAFCI_URL + f"/fondo/{fund_id}/clase/{class_id}"
        logger.info("Getting fund class from cafci")
        response = self.perform_request(url=cafci_funds_url)

        self.validated_cafci_response(response)

        parsed_response = response.get("data")
        logger.info("Got %s funds from cafci", len(parsed_response))

        formatted_data = self.format_fund_class_data(parsed_response)

        return formatted_data

    def format_fund_class_data(self, fund_class_data):
        """
        Format the fund class data to be inserted in the database.
        data example: https://api.cafci.org.ar/fondo/1222/clase/3924/ficha
        return:
        [
            class_name,
            fund_name,
            trading_currency,
            class_cafci_code,
            fund_cafci_code,
            rescue_time,
            risk_level,
            tna,
            tea,
            tem,
            monthly_performance,
            year_performance,
            updated,
            logo_url
        ]
        """
        class_name = fund_class_data.get('nombre')
        # Format the name to obtain the class name, example: D
        class_name_formated = class_name.split(" ")[-1]

        if len(class_name_formated) > 1:
            class_name_formated = None

        name = fund_class_data.get("fondo").get("nombre")

        class_id = fund_class_data.get('id')
        fund_id = fund_class_data.get('fondo').get('id')

        rescue_time = int(fund_class_data.get('diasLiquidacion')) * 24  # 3 * 24 = 72
        if rescue_time > 72:
            rescue_time = 72

        trading_currency = fund_class_data.get("fondo").get("monedaId")  # "1"
        # Format the trading currency to obtain the currency name, example: 1 -> ARS | 2 -> USD
        trading_currency = "ARS" if trading_currency == "1" else "USD"

        if trading_currency == "USD" and rescue_time == 0:
            rescue_time = 24

        risk_level = fund_class_data.get('fondo').get('tipoRentaId')
        # Format the rescue time to obtain the rescue time id, example: "Corto Plazo" -> 0
        risk_level = RISK_LEVEL_DICT.get(risk_level, 2)  # 0

        updated = get_current_time().strftime("%d-%m-%Y")

        fund_class_data = [
            class_name_formated,
            name,
            trading_currency,
            class_id,
            fund_id,
            rescue_time,
            risk_level,
            None,
            None,
            None,
            None,
            None,
            updated,
            None
        ]

        return fund_class_data
