"""Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved. 
SPDX-License-Identifier: MIT-0
"""
from src.rowProcessor import TableRowProcessor

class CBASavingsRowProcessor(TableRowProcessor):

    def process_row(self, row, statement_type, statement_name):
        print("Processing a CBA savings row")
        print(row)
        row['Date'] = super().get_date(statement_type, statement_name, row['Date'])
        if row['Credit']:
            row['Credit'] = super().convert_str_to_float(row['Credit'])
            row['Debit'] = 0
        if row['Debit']:
            row['Debit'] = super().convert_str_to_float(row['Debit'])
            row['Credit'] = 0
        del row['Balance']
        row_no_blank_keys = {k: v for k, v in row.items() if k}
        print(row_no_blank_keys)
        return row_no_blank_keys
        