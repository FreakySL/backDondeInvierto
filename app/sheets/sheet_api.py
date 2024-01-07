from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account




class APISpreadsheet:

	SCOPEs = ['https://www.googleapis.com/auth/spreadsheets']
	KEY = 'key.json'
	SPREADSHEET_ID = '1EDfxFQA4ncCaRl6G45oyufY8KNhiuFuGRbTb-JGsLzg'

	def __init__(self):
		self.creds = service_account.Credentials.from_service_account_file(self.KEY, self.SCOPES)
		self.service = build("sheets","v4", self.creds)
		self.sheet = self.service.spreadsheets()
    
	def get_all_rows(self):
		result = self.sheet.values().get(spreadsheetId=self.SPREADSHEET_ID, range='funds!A2:J').execute()
		array_rows = result.get('values',[])
		return array_rows

	def post_fund(self, values):
		response = self.sheet.values().append(
			spreadsheetId=self.SPREADSHEET_ID, 
			range='funds!A1', 
			valueInputOption='USER_ENTERED', 
			body={'values':values}
		).execute()
		print(f"Datos insertados correctamente.\n{(response.get('updates').get('updatedCells'))}")

	def array_row_to_dicctionary(self):
		return True

    def array_row_to_dicctionary(self):
        dictionary = {}
        array_rows = self.get_all_rows()

        for x in array_rows:
            new = {"class" : x[0], 
                "fund_name" : x[1], 
                "trading_currency" : x[2], 
                "fund_cafci_code" : x[4], 
                "rescue_time" : x[5], 
                "risk_level" : x[6], 
                "tem" : x[7], 
                "monthly_performance" : x[8], 
                "updated" : x[9], 
                "logo_url" : x[10], 
            }

            dictionary[x[3]] = new
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
         
def post_fund(array_row):
         
        result = sheet.values().append(spreadsheetId=SPREADSHEET_ID, range='A1', valueInputOption='USER_ENTERED', body={'values':array_row}).execute()
        print(f"Datos insertados correctamente.\n{(result.get('updates').get('updatedCells'))}")
         



              
              



"""
	def append(self, object : object) -> bool : 
		
		param:object -> Es cualquier objeto
		return : booleano (true o false)
	
		data = object.get_data_to_sheet()
		
        values = [data]

        result = sheet.values().append(spreadsheetId=SPREADSHEET_ID, range='A1', valueInputOption='USER_ENTERED', body={'values':values}).execute()
"""

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