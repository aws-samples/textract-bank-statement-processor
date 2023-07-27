"""Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved. 
SPDX-License-Identifier: MIT-0
"""
from abc import ABC, abstractmethod
from re import sub

class TableRowProcessor(ABC):

    @abstractmethod
    def process_row(self, row, statement_type, statement_name):
        """Processes a row in a table extracted
        by Textract, into an object in the standard 
        transaction format
        """
        pass

    def get_date(self, statement_type, filename, date):
        year = int(filename[9:13])
        if (statement_type == 'cba_bank'):
            mon = date[3:6]
            mon_last_year = ['Oct', 'Nov', 'Dec']
            if mon in mon_last_year:
                year = year - 1
            return date + ' ' + str(year)
        elif (statement_type == 'cba_cc'):
            year = int(filename[9:13])
            return date + ' ' + str(year)
        else:
            return date
    
    def convert_str_to_float(self, value):
        try:
            return float(sub(r'[^\d.]', '', value))
        except Exception as e:
            print(e)
            return 0.0
