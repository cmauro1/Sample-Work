# -*- coding: utf-8 -*-
"""
Created on Tue Sep 26 10:22:23 2023

@author: cjmauro
"""

import pyodbc as db
import numpy as np
import re #regex
import sys
import traceback

class SqlDataHandler():
       
       def __init__(self, sql_connector=None, field_map={}):

              #DB Connection
              self.sql_connector = sql_connector

              #Cursor used for executing statements in the DB
              if self.sql_connector:
                     self.cursor = self.sql_connector.cursor()
              else:
                     self.cursor = None
                     
              #key is the external field name
              #value is the corresponding field name in the target sql db
              if isinstance(field_map, dict):
                     self.field_map = field_map
              else:
                     self.raise_bad_field_map_exception()

              self.rows_to_insert = []


       def raise_bad_field_map_exception(self, bad_field_map):
              raise TypeError
              print(f'SqlDataHandler was expecting instance of dict.  Instead, {type(bad_field_map)} was given.\n')
              
       
       def set_sql_connector(self, sql_connector):
              self.sql_connector = sql_connector
              self.cursor = self.sql_connector.cursor()
                     
              
       def set_field_map(self, field_map):
              #key should be an external field name
              #value should be the corresponding field name from a table of the db specified by the connector
              if isinstance(field_map, dict):
                     self.field_map = field_map
              else:
                     self.raise_bad_field_map_exception()
                     

       def add_row_to_insert(self, row: dict):
              '''
              Use this method to stage rows to be inserted into a table.
              Once all desired rows have been added to the internal
              rows_to_insert list, call insert_rows_to_table() to execute
              the insert statement.
              '''
              #row is a dict where key is the external field name
              #and value is the row value
              self.rows_to_insert.append(self.build_values_to_insert_list(row))
              

       def insert_rows_to_table(self, row: dict, table_name: str):
              insert_statement = self.create_insert_statement(row, table_name) 
              try:
                     self.sql_connector.autocommit = False
                     self.cursor.fast_executemany = True
                     self.cursor.executemany(f'{insert_statement}', self.rows_to_insert)
              except:
                     traceback.print_exc()
                     print('There was an unexpected issue when executing the update statement.\nTerminating insert statement...')
                     print(f'{insert_statement} {self.rows_to_insert}\n\n')
                     self.cursor.rollback()
                     raise RuntimeError()
              else:
                     self.cursor.commit()
                     print(f'Insert statement committed to {table_name}')
              finally:
                     self.sql_connector.autocommit = True
                     self.cursor.fast_executemany = False
                     self.rows_to_insert.clear()
                     print(f'SQLDataHandler returned to initial state')
                
              
       def create_insert_statement(self, row, table_name):
              '''
              if self.row_is_empty(row):
                     #don't insert empty rows
                     insert_statement = ';'
              else:
              '''
              #this will not work if the order of objects in dict are changed or no longer guaranteed
              field_names = self.build_field_names_string(row)
              value_placeholders = self.build_value_placeholders_string(row)                     
              insert_statement = f'INSERT INTO {table_name} {field_names} VALUES {value_placeholders}'
              return insert_statement
       
       
       def row_is_empty(self, row):
              #Row is considered empty if it contains all null or missing values
              external_fields = tuple(row.keys())
              values = tuple(row.get(field) for field in external_fields)
              null_values = ('None', 'nan', '', None, np.NaN)
              is_empty = False
              for value in values:
                     if value not in null_values:
                            break
              else:
                     is_empty = True              
              return is_empty

       
       def scrub_data(func):
              '''
              Decorator function: takes function that returns data to be scrubbed, 
              converts it to a string, and removes characters commonly used for 
              sql-injection, such as single/double quotes, and parentheses.
              '''
              def wrapper(*args, **kwargs):
                     scrubbed_data = []
                     for dirty_data in func(*args, **kwargs):
                            scrubbed_data.append(re.sub('\"|\'|\(|\)', '', str(dirty_data)))
                     return scrubbed_data
              return wrapper
       
       
       def build_field_names_string(self, row):
              '''
              Takes a set of incoming field names and builds a string using the mapped 
              table-level field names.  The string is meant to be used in a sql insert 
              statement as the fields of the table that values are being inserted into.
              '''
              external_fields = tuple(row.keys())
              internal_fields = ''
              for field in external_fields:
                     internal_fields += f'{self.field_map.get(field.strip())},'
              internal_fields = f'({internal_fields[:-1]})' #put in parens and chop off last comma
              return internal_fields
       
       
       def build_value_placeholders_string(self, row):
              external_fields = tuple(row.keys())
              value_placeholders = ''
              for field in external_fields:
                     value_placeholders += '?,'
              value_placeholders = f'({value_placeholders[:-1]})' #put in parens and chop off last comma
              return value_placeholders

       @scrub_data
       def build_values_to_insert_list(self, row):
              external_fields = tuple(row.keys())
              values_to_insert = [str(row.get(field)) if field in ('Latitude', 'Longitude') else row.get(field) for field in external_fields]
              return values_to_insert


       
       def select_data_from_table(self, columns='Top 100 *', table=''):
              #Selects Top 100 * by default as a safety measure
              select_statement = self.create_select_statement(columns, table)
              result_set = self.get_result_set(select_statement)
              return result_set
              

       def create_select_statement(self, columns='Top 100 *', table=''):
              select_statement = f'SELECT {columns} FROM {table}'
              return select_statement
 
             
       '''
       If joins and where clause are needed, extend class 
       with create_[clause]_statement methods.
       '''


       def get_result_set(self, select_statement):
              print(select_statement)
              rows = self.cursor.execute(select_statement).fetchall()
              return rows


       def execute_stored_procedure(self, proc_name: str, *args: str):
              placeholders = ' '.join(['?' for arg in args])
              params = ' '.join([arg for arg in args])
              self.cursor.execute(f'{proc_name} {placeholders}', params)
