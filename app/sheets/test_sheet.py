from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account

from pdb import set_trace

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
KEY = 'key.json'
# Escribe aquí el ID de tu documento:
SPREADSHEET_ID = '1EDfxFQA4ncCaRl6G45oyufY8KNhiuFuGRbTb-JGsLzg'

creds = None
creds = service_account.Credentials.from_service_account_file(KEY, scopes=SCOPES)

service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()

#--------------------------------------------------------
#            GET
#--------------------------------------------------------


# Llamada a la api
result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range='funds!A2:J').execute()
# Extraemos values del resultado
values = result.get('values',[])
print(values)
