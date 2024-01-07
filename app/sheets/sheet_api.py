from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account


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
        result = self.sheet.values().get(spreadsheetId=self.SPREADSHEET_ID, range=f'{sheet_name}!{_range}').execute()
        array_rows = result.get('values', [])
        print(f"Obtenidos {len(array_rows)} datos de la hoja {sheet_name}")
        return array_rows

    def post_fund(self, values, sheet_name="funds", _range=FIST_CELL):
        response = self.sheet.values().append(
            spreadsheetId=self.SPREADSHEET_ID,
            range=f'{sheet_name}!{_range}',
            valueInputOption=self.APPEND_CONST,
            body={'values': values}
        ).execute()

        print(f"{response.get('updates').get('updatedCells')} celdas actualizadas")

    def response_to_dicctionary(self, response):
        dictionary = {}

        for fund in response:
            new = {
                "class": fund[0],
                "fund_name": fund[1],
                "trading_currency": fund[2],
                "fund_cafci_code": fund[4],
                "rescue_time": fund[5],
                "risk_level": fund[6],
                "tem": fund[7],
                "monthly_performance": fund[8],
                "updated": fund[9],
                "logo_url": fund[10],
            }

            dictionary[fund[3]] = new

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
                print("Found new fund ID: %s", x[3])
                new_funds_array.append(x)

        return new_funds_array


def test_format_dict():
    api = APISpreadsheet()
    response = api.get_all_rows()
    dictionary = api.response_to_dicctionary(response)
    print(dictionary)
