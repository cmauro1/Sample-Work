# -*- coding: utf-8 -*-
"""
Created on Fri Sep  8 10:13:52 2023

@author: cjmauro
"""

import pandas as pd

class ExcelDataHandler():
       
       def __init__(self, dataframe = pd.DataFrame(), filepath = '', sheetname = ''):
              
              if isinstance(dataframe, pd.DataFrame):
                     self.dataframe = dataframe
                     self.filepath = str(filepath)
                     self.sheetname = str(sheetname)
                     self.row_cursor = 0 #private, used as index to access next row in dataframe
              else:
                     self.raise_bad_dataframe_exception(dataframe)
                     
       def raise_bad_dataframe_exception(self, bad_dataframe):
              raise TypeError(f'ExcelDataHandler expecting instance of pandas.DataFrame. Instead, {type(bad_dataframe)} was given.\n')
              
       def set_dataframe(self, dataframe):
              if isinstance(dataframe, pd.DataFrame):
                     self.drop_old_dataframe()
                     self.insert_new_dataframe(dataframe)
                     self.reset_row_cursor_to_zero()
              else:
                     self.raise_bad_dataframe_exception()
                     
       def drop_old_dataframe(self):
              all_columns = self.dataframe.columns
              self.dataframe.drop(all_columns, axis=1, inplace=True)  #axis, 1 for columns and 0 for rows
              
       def insert_new_dataframe(self, dataframe):
              self.dataframe = dataframe
              
       def get_next_row(self):
              next_row = self.dataframe.iloc[self.row_cursor]
              self.increment_row_cursor()
              return next_row
       
       def increment_row_cursor(self):
              self.row_cursor += 1
              #When the final row has been processed, reset cursor to 0
              if self.row_cursor == len(self.dataframe.index):
                     self.reset_row_cursor_to_zero()
                     
       def reset_row_cursor_to_zero(self):
              self.row_cursor = 0
              
       def set_filepath(self, filepath: str):
              self.filepath = str(filepath)
              
       def get_filepath(self) -> str:
              return self.filepath
       
       def __len__(self):
              num_rows = len(self.dataframe.index)
              return num_rows

       def __getitem__(self, position):
              return self.dataframe.iloc[position]
       
       def insert_new_column_no_constraints(self, name, col_index=0,value=''):
              self.dataframe.insert(loc=col_index, column=name, value=value, allow_duplicates=True)
              
       def set_sheetname(self, sheetname):
              self.sheetname = str(sheetname)
              
       def get_sheetname(self):
              return self.sheetname

       def get_dataframe(self):
              return self.dataframe
