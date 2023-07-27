"""Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved. 
SPDX-License-Identifier: MIT-0
"""
from src.rowProcessor import TableRowProcessor

class CBACCRowProcessor(TableRowProcessor):

    def process_row(self, row, statement_type, statement_name):
        print(row)
        print("Processing a CBA CC row")
        row['Date'] = super().get_date(statement_type, statement_name, row['Date'])
        if 'Transaction Details' in row:
            row['Transaction'] = row['Transaction Details']
            del row['Transaction Details']
            if row['Amount (A$)'].endswith('-'):
                credit = row['Amount (A$)'][:-1]
                row['Credit'] = super().convert_str_to_float(credit)
                row['Debit'] = 0
                del row['Amount (A$)']
            else:
                row['Debit'] = super().convert_str_to_float(row['Amount (A$)'])
                row['Credit'] = 0
                del row['Amount (A$)']
        if 'Transaction details' in row:
            row['Transaction'] = row['Transaction details']
            del row['Transaction details']
            if row['Amount (A$)'].endswith('-'):
                credit = row['Amount (A$)'][:-1]
                row['Credit'] = super().convert_str_to_float(credit)
                row['Debit'] = 0
                del row['Amount (A$)']
            else:
                row['Debit'] = super().convert_str_to_float(row['Amount (A$)'])
                row['Credit'] = 0
                del row['Amount (A$)']
        if 'Transaction details Amount (A$)' in row:
            tokens = row['Transaction details Amount (A$)'].split()
            amount = tokens[-1]
            row['Debit'] = super().convert_str_to_float(amount)
            row['Credit'] = 0
            row['Transaction'] = row['Transaction details Amount (A$)']
            del row['Transaction details Amount (A$)']
        if 'Transaction Details Amount (A$)' in row:
            tokens = row['Transaction Details Amount (A$)'].split()
            amount = tokens[-1]
            row['Debit'] = super().convert_str_to_float(amount)
            row['Credit'] = 0
            row['Transaction'] = row['Transaction Details Amount (A$)']
            del row['Transaction Details Amount (A$)']

        row_no_blank_keys = {k: v for k, v in row.items() if k}
        print(row_no_blank_keys)
        return row_no_blank_keys
        