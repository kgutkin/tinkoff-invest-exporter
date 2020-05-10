from __future__ import print_function

import os.path
import pickle
import re
from collections import defaultdict

from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from db_operations import export2db, read_transactions_from_db, read_header_names_from_db, read_settings_from_db


class TinkoffExporter:

    def __init__(self, args):
        # If modifying these scopes, delete the file token.pickle.
        self.scopes = ['https://www.googleapis.com/auth/spreadsheets']
        self.currency_index = 10
        self.range_pattern = re.compile(r'.*!\w(\d*):\w(\d*)')

        settings = read_settings_from_db()

        self.spreadsheet_id = args.spreadsheetId or settings.get('spreadsheetId')
        self.currency_template_sheet_id = {'RUB': args.rubSheetId or settings.get('rubSheetId'),
                                           'USD': args.usdSheetId or settings.get('usdSheetId')}
        self.credentials_path = args.credentials or settings.get('credentials')
        self.report_file_name = args.fileName

        if self.spreadsheet_id is None:
            raise Exception('spreadsheet_id required! You ned specify it.\nRun with --help for more information')
        if self.currency_template_sheet_id['RUB'] is None:
            raise Exception('rubSheetId required! You ned specify it.\nRun with --help for more information')
        if self.currency_template_sheet_id['USD'] is None:
            raise Exception('usdSheetId required! You ned specify it.\nRun with --help for more information')
        if self.credentials_path is None:
            raise Exception('credentials required! You ned specify it.\nRun with --help for more information')

        self.sheets_api = self.__get_google_sheets_api()

    def export(self):
        export2db("./files/broker_rep.xlsx")
        all_transactions = read_transactions_from_db()

        dictionary = defaultdict(list)
        for item in all_transactions:
            item_list = list(item)
            dictionary[item_list[8]].append(item_list)

        spreadsheet = self.sheets_api.get(spreadsheetId=self.spreadsheet_id).execute()
        sheet_list = spreadsheet['sheets']
        google_sheet_titles = [sheet['properties']['title'] for sheet in sheet_list]

        header_names = read_header_names_from_db()

        count_sheets = len(google_sheet_titles)
        for (ticker, transactions) in dictionary.items():
            values = [row[:len(header_names)] for row in transactions]
            if ticker not in google_sheet_titles:
                self.__duplicate_sheet(count_sheets, ticker,
                                       self.currency_template_sheet_id[list(transactions[0])[self.currency_index]])
                count_sheets += 1
                self.__append_values_to_sheet(values, ticker, 'create new sheet "%s" with %s rows')
            else:
                self.__append_values_to_sheet(values, ticker, 'add to sheet "%s" %s rows')

    def __get_google_sheets_api(self):
        creds = None
        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(self.credentials_path, self.scopes)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('token.pickle', 'wb') as token:
                pickle.dump(creds, token)

        service = build('sheets', 'v4', credentials=creds)

        # Call the Sheets API
        return service.spreadsheets()

    def __append_values_to_sheet(self, data, ticker, log_template):
        self.sheets_api.values().append(spreadsheetId=self.spreadsheet_id, range=f"{ticker}!A6", body={"values": data},
                                        valueInputOption="RAW").execute()

        # uncomment next if need format Google Sheet cells as #,##0.00[{currency_symbol}]
        # currency_symbol = "₽" if transactions[0][CURRENCY_INDEX] == "RUB" else "$"
        # self.__format_number_cells(self, currency_symbol, result, sheet_id, sheets_api)
        print(log_template % (ticker, len(data)))

    def __format_number_cells(self, currency_symbol, result, sheet_id):
        self.sheets_api.batchUpdate(spreadsheetId=self.spreadsheet_id, body={
            'requests': [{
                'repeatCell': {
                    'range': {
                        'sheetId': sheet_id,
                        'startColumnIndex': price_column_index,
                        'endColumnIndex': price_column_index,
                        'startRowIndex': int(self.range_pattern.findall(result['updatedRange'])[0][0]) - 1,
                        'endRowIndex': int(self.range_pattern.findall(result['updatedRange'])[0][1]) - 1
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'numberFormat': {
                                'type': 'CURRENCY',
                                'pattern': f'#,##0.00[{currency_symbol}]'
                            }
                        }
                    },
                    'fields': 'userEnteredFormat.numberFormat'
                }
            } for price_column_index in [9, 12, 13, 14, 16]]
        }).execute()

    def __duplicate_sheet(self, sheet_index, sheets_api, new_sheet_name, source_sheet_id):
        return sheets_api.batchUpdate(spreadsheetId=self.spreadsheet_id, body={
            'requests': [{
                "duplicateSheet": {
                    "sourceSheetId": source_sheet_id,
                    "newSheetName": new_sheet_name,
                    "insertSheetIndex": sheet_index
                }
            }]
        }).execute()
