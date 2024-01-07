from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.errors import HttpError


from ..common.utils import (
    get_logger,
    get_current_time,
)

logger = get_logger(__name__)


class APISpreadsheet:

    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    KEY = 'app/sheets/key.json'
    SPREADSHEET_ID = '1EDfxFQA4ncCaRl6G45oyufY8KNhiuFuGRbTb-JGsLzg'
    GOOGLE_API = "sheets"
    GOOGLE_API_VERSION = "v4"
    FIST_CELL = "A1"
    APPEND_CONST = "USER_ENTERED"

    def __init__(self):
        self.creds = service_account.Credentials.from_service_account_file(self.KEY, scopes=self.SCOPES)
        self.service = build(self.GOOGLE_API, self.GOOGLE_API_VERSION, credentials=self.creds)
        self.sheet = self.service.spreadsheets()

    def get_all_rows(self, sheet_name="funds", _range="A1:K"):
        try:
            result = self.sheet.values().get(
                spreadsheetId=self.SPREADSHEET_ID,
                range=f'{sheet_name}!{_range}'
            ).execute()

            array_rows = result.get('values', [])
            logger.info(f"Obtenidos {len(array_rows)} datos de la hoja {sheet_name}")

        except HttpError as error:
            logger.error("Error al obtener los datos de la hoja: %s", error)
            return

        return array_rows

    def post_data(self, values, sheet_name="funds", _range=FIST_CELL):
        try:
            body = {'values': values}
            response = self.sheet.values().append(
                spreadsheetId=self.SPREADSHEET_ID,
                range=f'{sheet_name}!{_range}',
                valueInputOption=self.APPEND_CONST,
                body=body
            ).execute()

            logger.info(f"{response.get('updates').get('updatedCells')} celdas añadidas")

        except HttpError as error:
            logger.error("Error al actualizar la hoja: %s", error)
            return

        logger.info(f"{response.get('updates').get('updatedCells')} celdas actualizadas")

    def response_to_dicctionary(self, response):
        dictionary = {}

        for x in response:
            new = {
                "class": x[0],
                "fund_name": x[1],
                "trading_currency": x[2],
                "fund_cafci_code": x[4],
                "rescue_time": x[5],
                "risk_level": x[6],
                "tem": x[7],
                "monthly_performance": x[8],
                "updated": x[9],
                "logo_url": x[10],
            }

            dictionary[x[3]] = new

        return dictionary

    def get_all_rows_formated(self):
        response = self.get_all_rows()
        dictionary = self.response_to_dicctionary(response)
        return dictionary

    def find_new_funds(self, array_row, dictionary):
        new_funds_array = []

        for x in array_row:

            if x[3] in dictionary:
                continue

            else:
                logger.info("Found new fund ID: %s", x[3])
                new_funds_array.append(x)

        return new_funds_array


def test_format_dict():
    api = APISpreadsheet()
    response = api.get_all_rows()
    dictionary = api.response_to_dicctionary(response)
    logger.info(dictionary)
