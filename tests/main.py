import unittest
from unittest.mock import patch, MagicMock
from app.sheets import APISpreadsheet
from app.models import FundClassParser
from decimal import Decimal
from emoji import emojize


class TestAPISpreadsheet():
    def __init__(self):
        self.api = APISpreadsheet()

    def assertEqual(self, result, expected_result):
        if result == expected_result:
            print(emojize(":check_mark_button: Test passed successfully"))
        else:
            print(emojize(":cross_mark: Test failed"))
            print(f"Result: {result} \nExpected: {expected_result}")

    def test_get_data_for_funds(self):
        result = self.api.get_data(sheet_name="funds", _range="A1:K1")
        expected_data = [['class', 'name', 'trading_currency', 'class_cafci_code', 'fund_cafci_code',
                          'rescue_time', 'risk_level', 'tem', 'monthly_performance', 'updated', 'logo_url']]

        self.assertEqual(result, expected_data)

    def test_post_data_for_funds(self):
        result = self.api.post_data(values=[['test data']], sheet_name="funds", _range="M2:N2")
        expected_data = 1

        self.assertEqual(result, expected_data)

    def test_update_data_for_funds(self):
        result = self.api.update_data(values=[[""]], sheet_name="funds", _range="M2:N2")
        expected_data = 1

        self.assertEqual(result, expected_data)

    def test_response_to_dictionary(self):
        response = [
            ['class1', 'name1', 'currency1', 'code1', 'code_fund1', 'rescue1',
                'risk1', 'tem1', 'performance1', 'updated1', 'logo1'],
            ['class2', 'name2', 'currency2', 'code2', 'code_fund2', 'rescue2',
                'risk2', 'tem2', 'performance2', 'updated2', 'logo2']
        ]

        expected_result = {
            "code1": {
                "class": "class1",
                "name": "name1",
                "trading_currency": "currency1",
                "fund_cafci_code": "code_fund1",
                "rescue_time": "rescue1",
                "risk_level": "risk1",
                "tem": "tem1",
                "monthly_performance": "performance1",
                "updated": "updated1",
                "logo_url": "logo1"
            },
            "code2": {
                "class": "class2",
                "name": "name2",
                "trading_currency": "currency2",
                "fund_cafci_code": "code_fund2",
                "rescue_time": "rescue2",
                "risk_level": "risk2",
                "tem": "tem2",
                "monthly_performance": "performance2",
                "updated": "updated2",
                "logo_url": "logo2"
            }
        }

        result = self.api.response_to_dicctionary(response)

        self.assertEqual(result, expected_result)


class TestFundClassParser():
    def __init__(self):
        self.parser = FundClassParser()

    def assertEqual(self, result, expected_result):
        if result == expected_result:
            print(emojize(":check_mark_button: Test passed successfully"))
        else:
            print(emojize(":cross_mark: Test failed"))
            print(f"Result: {result} \nExpected: {expected_result}")

    def test_get_tem(self):
        result = self.parser.get_tem(Decimal(100), Decimal(110))
        expected_result = Decimal(40.0)

        self.assertEqual(result, expected_result)

    def test_get_max_range(self):
        result = self.parser.get_max_range()
        expected_result = "A1:K"

        self.assertEqual(result, expected_result)

    def test_get_sheet(self):
        result = self.parser.get_sheet()
        expected_result = "funds"

        self.assertEqual(result, expected_result)

    def test_get_fund_codes_range(self):
        result = self.parser.get_fund_codes_range()
        expected_result = "D2:E"

        self.assertEqual(result, expected_result)

    def test_get_tem_monthly_updated_range(self):
        result = self.parser.get_tem_monthly_updated_range()
        expected_result = "H2:I"

        self.assertEqual(result, expected_result)

    def test_get_monthly_performance(self):
        result = self.parser.get_monthly_performance()
        expected_result = None

        self.assertEqual(result, expected_result)

    def test_get_index(self):
        result = self.parser.get_index()
        expected_result = "class_cafci_code"

        self.assertEqual(result, expected_result)

    def test_get_seven_days_price(self):
        with patch("app.models.funds.FundClassParser.get_seven_days_price") as mock:
            mock.return_value = (Decimal(100), Decimal(110))
            result = self.parser.get_seven_days_price(class_id="4", fund_id="2")
            expected_result = (Decimal(100), Decimal(110))

            self.assertEqual(result, expected_result)

    def test_get_check_cafci_connection(self):
        result = self.parser.get_cafci_ficha_default()
        expected_result = True

        self.assertEqual(result, expected_result)

    def test_cafci_format(self):
        response = {
            "clasificacionVieja": "Renta Fija",
            "regionVieja": "Argentina",
            "horizonteViejo": "Corto Plazo",
            "tipoEscision": "No aplicable",
            "id": "1",
            "createdAt": "2017-07-07T22:09:18.000Z",
            "updatedAt": "2021-01-13T16:00:07.000Z",
            "nombre": "Alianza de Capitales",
            "codigoCNV": "52",
            "objetivo": "El objetivo es superar el rendimiento de la tasa BADLAR",
            "resolucionParticular": "<Faltante>",
            "fechaResolucionParticular": "1990-01-01T00:00:00.000Z",
            "fechaInscripcionRPC": "1990-01-01T00:00:00.000Z",
            "estado": "1",
            "etapaLiquidacion": None,
            "tipoRentaId": "3",
            "monedaId": "1",
            "regionId": "1",
            "durationId": "3",
            "benchmarkId": "9",
            "mmIndice": False,
            "mmPuro": False,
            "valuacion": "M",
            "ci49": False,
            "diasLiquidacion": "3",
            "indice": False,
            "horizonteId": "1",
            "sociedadGerenteId": "11",
            "sociedadDepositariaId": "65",
            "inicio": "1992-06-22T00:00:00.000Z",
            "tipoFondoId": "1",
            "tipoDinero": "No Aplica",
            "tipoRentaMixtaId": None,
            "excentoTasa": False,
            "d569": False,
            "d569FondoId": None,
            "fechaCierreBalances": "1900-12-30T04:16:48.000Z",
            "tipoRenta": {
                    "id": "3",
                    "createdAt": "1990-01-01T00:00:00.000Z",
                    "updatedAt": "1990-01-01T00:00:00.000Z",
                    "nombre": "Renta Fija",
                    "codigoCafci": "RF",
                    "orden": "B  ",
                    "parametroPorcentual": 5
                },
            "clase_fondos": [
                {
                    "id": "1",
                    "createdAt": "2017-07-07T22:09:24.000Z",
                    "updatedAt": "2017-07-07T22:09:24.000Z",
                    "nombre": "Alianza de Capitales",
                    "inversionMinima": "1",
                    "honorarioIngreso": 0,
                    "honorarioRescate": 0,
                    "honorarioTransferencia": 0,
                    "honorarioAdministracionGerente": 2,
                    "honorarioAdministracionDepositaria": 0.4,
                    "gastoOrdinarioGestion": 0,
                    "honorarioExito": False,
                    "monedaId": "1",
                    "rg384": False,
                    "liquidez": True,
                    "suscripcion": True,
                    "reexpresa": False,
                    "nulo": False,
                    "tipoClaseId": "1",
                    "fondoId": "1",
                    "tickerBloomberg": "ALIACAP   ",
                    "tickerISIN": None,
                    "tickerFIGI": None,
                    "repatriacion": None
                }
            ]
        }
        data = self.parser.get_fund_classes_by_fund_group(response)

        expected_data = [
            ['Capitales', 'Alianza de Capitales', 'ARS', '1', '1', 24, 0, None, None, "08-01-2024", None]
        ]

        self.assertEqual(data, expected_data)


if __name__ == '__main__':
    print("Testing APISpreadsheet")
    test_case = TestAPISpreadsheet()
    print(emojize(":rocket: Starting tests"))
    test_case.test_get_data_for_funds()
    print(emojize(":hourglass_not_done: Testing post_data_for_funds"))
    test_case.test_post_data_for_funds()
    print(emojize(":hourglass_not_done: Testing update_data_for_funds"))
    test_case.test_update_data_for_funds()
    print(emojize(":hourglass_not_done: Testing response_to_dictionary"))
    test_case.test_response_to_dictionary()

    print("Testing FundClassParser")
    test_case = TestFundClassParser()
    print(emojize(":hourglass_not_done: Testing get_tem"))
    test_case.test_get_tem()
    print(emojize(":hourglass_not_done: Testing get_max_range"))
    test_case.test_get_max_range()
    print(emojize(":hourglass_not_done: Testing get_sheet"))
    test_case.test_get_sheet()
    print(emojize(":hourglass_not_done: Testing get_fund_codes_range"))
    test_case.test_get_fund_codes_range()
    print(emojize(":hourglass_not_done: Testing get_tem_monthly_updated_range"))
    test_case.test_get_tem_monthly_updated_range()
    print(emojize(":hourglass_not_done: Testing get_monthly_performance"))
    test_case.test_get_monthly_performance()
    print(emojize(":hourglass_not_done: Testing get_index"))
    test_case.test_get_index()
    print(emojize(":hourglass_not_done: Testing get_seven_days_price"))
    test_case.test_get_seven_days_price()
    print(emojize(":hourglass_not_done: Testing get_check_cafci_connection"))
    test_case.test_get_check_cafci_connection()
    print(emojize(":hourglass_not_done: Testing cafci_format"))
    test_case.test_cafci_format()

    print(emojize(":rocket: Tests finished"))
