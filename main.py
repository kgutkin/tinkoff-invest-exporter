from __future__ import print_function
import argparse
from db_operations import store_settings_to_db
from exporter import TinkoffExporter


def parse_args():
    parser = argparse.ArgumentParser(
        description='Export transactions data from Tinkoff invest Excel report to Google Sheets. '
                    'Please, provide spreadsheetId, rubSheetId, usdSheetId, credentials for correct program run.')
    parser.add_argument('fileName', metavar='fileName', type=str,
                        help='path to Tinkoff invest Excel report file name')
    parser.add_argument('--credentials', '-c', dest='credentials',
                        help='CREDENTIALS - path to credentials.json file for access to Google API')
    parser.add_argument('--spreadsheet-id', '-id', dest='spreadsheetId',
                        help='SPREADSHEETID - Google Sheet Spreadsheet ID in which will be store transactions data')
    parser.add_argument('--store-args-to-db', dest='storeArgs', action='store_true', help='store args to sqlite DB')
    args = parser.parse_args()

    if args.storeArgs:
        store_settings_to_db(args.__dict__)

    return args


if __name__ == '__main__':
    TinkoffExporter(parse_args()).export()
