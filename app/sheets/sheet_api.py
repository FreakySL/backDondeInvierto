from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account




class APISpreadsheet:

	SCOPEs = ['https://www.googleapis.com/auth/spreadsheets']
	KEY = 'key.json'
	SPREADSHEET_ID = '1EDfxFQA4ncCaRl6G45oyufY8KNhiuFuGRbTb-JGsLzg'

	def __init__(self):
		self.creds = None
		self.creds = service_account.Credentials.from_service_account_file(self.KEY, self.SCOPES)
		self.service = build("sheets","v4",creds)
		self.sheet = service.spreadsheets()


	def get_rows():

		#--------------------------------------------------------
		#            GET
		#--------------------------------------------------------


		# Llamada a la api
		result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range='Hoja 1!A1:H1').execute()
		# Extraemos values del resultado
		values = result.get('values',[])



	def append(self, object : object) -> bool : 
		"""
		param:object -> Es cualquier objeto
		return : booleano (true o false)
		"""
		data = object.get_data_to_sheet()
		
        values = [data]

        result = sheet.values().append(spreadsheetId=SPREADSHEET_ID,
									    range='A1',
									    valueInputOption='USER_ENTERED', 
										body={'values':values}).execute()
	

"""
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
result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range='Hoja 1!A1:H6').execute()
# Extraemos values del resultado
values = result.get('values',[])
print(values)

#--------------------------------------------------------
#            APPEND
#--------------------------------------------------------


# Debe ser una matriz por eso el doble [[]]
values = [['Chavo','Chavo2'],['Quico']]
# Llamamos a la api

result = sheet.values().append(spreadsheetId=SPREADSHEET_ID,
							range='A1',
							valueInputOption='USER_ENTERED',
							body={'values':values}).execute()
print(f"Datos insertados correctamente.\n{(result.get('updates').get('updatedCells'))}")

"""