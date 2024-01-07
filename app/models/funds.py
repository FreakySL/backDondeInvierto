import json
import requests
from decimal import Decimal
from multiprocessing import (
    Pool,
)

from ..common.utils import (
    get_logger,
    get_current_time,
)


logger = get_logger(__name__)


class FundClassParser():
    """Clase de una Serie de Fondo, matcheado con un asset.
    sheet example:
    | class | fund_name | trading_currency | class_cafci_code | fund_cafci_code | rescue_time | risk_level | tne | monthly_performance | updated | logo_url
    """
    SHEET = "funds"
    COLUMN_MAX_RANGE = "A1:K"
    BASE_CAFCI_URL = "https://api.cafci.org.ar"

    # Create the init
    def __init__(self,):
        pass

    @staticmethod
    def calculate_tem(self, initial_price: Decimal, final_price: Decimal) -> Decimal:
        """
        Calculate TEM from data.
        param: initial_price: Decimal
        param: final_price: Decimal
        return: TEM: Decimal
        """
        tem = ((final_price - initial_price) / initial_price) * 100 * 4  # 4 is the number of periods in a month

        # Return the TEM
        return tem

    def get_max_range(self):
        return self.COLUMN_MAX_RANGE

    def get_sheet(self):
        return self.SHEET

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
        cafci_funds_url = self.BASE_CAFCI_URL + "/fondo?estado=1&include=gerente,tipoRenta,region,benchmark,clase_fondo&limit=0"
        logger.info("Getting all funds from cafci")
        response = requests.get(cafci_funds_url)

        self.validated_cafci_response(response)

        parsed_response = response.json()["data"]
        logger.info("Got %s funds from cafci", len(parsed_response))

        return parsed_response

    def get_or_create_fund_classes_by_fund_group(self, fund_group_data, create=False):
        fund_classes = []
        for fund in fund_group_data.get("clase_fondos"):
            class_id = fund_group_data.get('id')
            fund_id = fund.get('id')
            updated = get_current_time()

            logger.info("Trayendo data de clase_id %s", class_id)

            cafci_fund_response = requests.get(
                f"{self.BASE_CAFCI_URL}/fondo/{class_id}/clase/{fund_id}/ficha"
            )

            self.validated_cafci_response(cafci_fund_response)

            fund_name = cafci_fund_response.json()["data"].get("model").get("nombre")
            trading_currency = cafci_fund_response.json()["data"].get("model").get("moneda")
            rescue_time = cafci_fund_response.json()["data"].get("model").get("plazoRescate")
            risk_level = cafci_fund_response.json()["data"].get("model").get("nivelRiesgo")

            # Crear datos de fondo para la request a nuestra API

            logger.info(f'Creando fondo codigo: {fund_id} - {fund_name}')

            # Crear data de fondo ejemplo: [class_id, fund_name, trading_currency, class_cafci_code, fund_cafci_code, rescue_time, risk_level, tne, monthly_performance, updated, logo_url]
            fund_class_data = [class_id, fund_name, trading_currency, class_id, fund_id, rescue_time, risk_level, None, None, updated, None]

            if create:
                # Crear fondo en nuestra API
                # return crear_fondo_google_sheet(fund_class_data)
                return "A"
            else:
                fund_classes.append(fund_class_data)

        return fund_classes

    def create_all_funds(self):
        response = self.get_all_fund_groups()

        with Pool(processes=16) as pool:
            pool.map(self.get_or_create_fund_classes_by_fund_group, response, create=True)

    def get_all_funds(self):
        response = self.get_all_fund_groups()

        for fund_group in response:
            fund_classes = self.get_or_create_fund_classes_by_fund_group(fund_group, create=False)

        return fund_classes
