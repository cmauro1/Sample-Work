# -*- coding: utf-8 -*-
"""
Created on Wed Aug 30 20:23:06 2023

@author: cjmauro
"""

import os
import sys
import time
import traceback
import pandas as pd
import numpy as np
import pyodbc as db
import tkinter as tk
from tkinter.ttk import *
from exceldatahandler import ExcelDataHandler
from sqldatahandler import SqlDataHandler
from serverloginwindow import ServerLoginWindow

print("""
                                _ _ _ _       
                               | (_) | |      
  __ _ _ __ _ __ ___   __ _  __| |_| | | ___  
 / _` | '__| '_ ` _ \\ / _` |/ _` | | | |/ _ \\ 
| (_| | |  | | | | | | (_| | (_| | | | | (_) |
 \\__,_|_|  |_| |_| |_|\\__,_|\\__,_|_|_|_|\\___/  


Welcome to the Armadillo Routesheet ETL.

I wrote this script to load a complex series of Routesheets into SQL Server
in order to complete a data migration project.

Instructions:

1. All Excel files being loaded should be available in a folder than can be accessed
by this program.

2. The field map variable below under the main() function should be edited.
This variable is a dictionary, where the keys are the column headers in the
Excel files and the values are the mapped database table fields.
(Note: if you have not updated this variable yet, please exit the program
and do this!)

3. Two tiny windows should appear on screen. I later opted out from the
tkinter windows in favor of console input once I figured out how to use
trusted connection.

3a. Complete the server login window first by entering the server domain,
database name, and your login credentials.

3b. Second, complete the folder path window, passing the relative folder
path to the Excel files that you prepared earlier.

4. All instances of {table} in this script need to be replaced with actual table names.

5. Sit back and watch as your data is loaded into SQL Server.

""")

      

directory = ''

def get_excel_filenames_from_directory(directory: str) -> list:
    '''Loops over given directory.  Returns collection of excel files.'''
    files = get_filenames_from_directory(directory)
    excel_files = filter_for_excel_files(files)
    return excel_files

def get_filenames_from_directory(directory: str) -> list:
    '''Retrieves the filenames from a given directory.'''
    files = []
    for filename in os.scandir(directory):
        if filename.is_file():
            files.append(filename.name)
    return files

def filter_for_excel_files(files: list) -> list:
    '''Takes a collection of filenames and expels non-excel files.
       Returns collection of excel filenames.'''
    for file in files:
        #if not an excel file, remove from collection
        if file.find('.xls') == -1:
            files.remove(file)
    return files

def add_required_columns(exceldatahandler, importid):
       #adds ImportID, Branch, Route, and ServiceDay columns - Armadillo specific
       add_route_column(exceldatahandler)
       add_branch_column(exceldatahandler)
       add_importid_column(exceldatahandler, importid)
       add_serviceday_column(exceldatahandler)
       
def add_route_column(exceldatahandler):
       route = exceldatahandler.get_sheetname()
       exceldatahandler.insert_new_column_no_constraints(name='Route', value=route)
       
def add_branch_column(exceldatahandler):
       branch = parse_branch(exceldatahandler)
       exceldatahandler.insert_new_column_no_constraints(name='Branch', value=branch)
       
def parse_branch(exceldatahandler):
       branch = ''
       filepath = exceldatahandler.get_filepath()
       if   'CLB' in filepath:
              branch = 'CLB' #columbus
       elif 'PCF' in filepath:
              branch = 'PCF' #point comfort
       elif 'SWE' in filepath:
              branch = 'SWE' #sweeney
       return branch
    
def add_importid_column(exceldatahandler, importid: int):
    exceldatahandler.insert_new_column_no_constraints(name='ImportID', value=importid)

def get_importid(sqldatahandler) -> int:
    #Unpack value from list of tuple returned by method
    importid = sqldatahandler.select_data_from_table(columns='ImportID', table='dbo.MostRecentImport')[0][0]
    return importid

def add_serviceday_column(exceldatahandler):
       serviceday = get_serviceday(exceldatahandler)
       exceldatahandler.insert_new_column_no_constraints(name='ServiceDay', value=serviceday)

def get_serviceday(exceldatahandler):
       '''
       This is 50% validation tool, 50% method to extract service day from filepath.
       Note to self: Lets see if I can consolidate this code in a more pythonic format.
       '''
       days = ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday')
       filepath = exceldatahandler.get_filepath()
       serviceday = ''
       for day in days:
              if day in filepath:
                     serviceday = day
                     break
       return serviceday

def main():  
    '''
    Begin by connecting to the SQL Server Database.  This is done first so the
    connection is made one time no matter how many Excel files need to be read in.
    '''
    field_map = {'input_field': 'mapped_field'}
    
    sql_data = SqlDataHandler(field_map=field_map)
    server_login = ServerLoginWindow(sql_data)

    '''
    Prompt user to enter path to folder containing Excel files needing to be 
    loaded into the SQL Server Database.  For each Excel file, read in each
    sheet in the workbook as a Pandas Dataframe.  ExcelDataHandler passes the
    Excel data to SqlDataHandler which loads it into the SQL Server Database.
    '''

    #initialize gui window
    root = tk.Tk()
    frame = Frame(root, padding=10)
    frame.grid()

    directory_input= tk.StringVar(root)
    
    directory_label = Label(frame, text='Enter directory path').grid(column=0, row=1)
    directory_entry = Entry(frame, textvariable=directory_input, width=20).grid(column=1, row=1)

    #submit button
    Button(frame, text='Submit', command=root.destroy).grid(column=1, row=2)  
    root.mainloop()

    directory_path = directory_input.get()
    routesheets_files = get_excel_filenames_from_directory(directory_path)

    xl_data = ExcelDataHandler()    

    time_start = time.perf_counter() # benchmarking runtime: total_duration = time_end - time_start

    #Set ImportID for this batch of routesheets.
    #Must be done before looping over files to ensure entire batch is loaded with same ID.
    importid = get_importid(sql_data) 

    #Report failed inserts upon completion
    routesheets_failed_to_insert = []
    
    #for each workbook in the given directory, 
    #read in worksheet as a dataframe.
    for file in routesheets_files:

        filepath = f'{directory_path}\{file}'
        
        workbook = pd.ExcelFile(filepath)
        sheet_names = workbook.sheet_names

        with workbook as xls:
        
            for current_sheet in sheet_names:
                print(f'Currently working on: {file} - {current_sheet}\n')

                #temporarily suppress Pandas FutureWarning about use of pd.dataframe.replace()
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore', category = FutureWarning)

                    #Read in dataframe and throw out all of the empty rows
                    dataframe = pd.read_excel(xls, sheet_name = current_sheet, dtype=object).dropna(thresh=10).replace(np.nan, 0)                        
                    dataframe.drop(labels = dataframe.columns[dataframe.columns.str.contains('unnamed',case=False)], axis = 'columns', inplace = True)
                                    
                    xl_data.set_dataframe(dataframe)
                    xl_data.set_filepath(filepath)
                    xl_data.set_sheetname(current_sheet)
                    add_required_columns(xl_data, importid)


                    if xl_data: #don't attempt an import if routesheet doesnt actually contain data
                        try:
                            for row in xl_data:
                                sql_data.add_row_to_insert(row)
                            sql_data.insert_rows_to_table(xl_data[:1], '{table}')
                            print(f'{file} - {current_sheet} inserted successfully into {table}\n\n')
                        except:
                            traceback.print_exc()
                            routesheets_failed_to_insert.append(f'{file} - {current_sheet}')
                            print(f'{file} - {current_sheet} failed to be inserted into {table}\n'
                                + 'This most commonly indicates one of the following issues:\n'
                                + ' *Column misalignment: look for rows where the lat/longs dont line up with the rest\n'
                                + ' *Longitudes have the negative sign (-) in the wrong spot: this seems to happen from time to time\n'
                                + ' *Duplicate column headers: one must be changed or deleted, otherwise the insert statement gets jacked up\n'  
                                + f'Please go back and examine {file} - {current_sheet} for inconsistancies'
                                + ' in the column headers and the data.\n\n')
                        finally:
                            continue
                
                
        workbook.close()

    # benchmarking runtime
    time_end = time.perf_counter() 
    total_duration = round(time_end - time_start, 4)

    print(f'Total runtime: {total_duration} seconds\n'
          + f'Routesheet import {importid} complete.\n\n')

    if routesheets_failed_to_insert:
        print(f'Please check on the following routesheets that failed to insert:\n {routesheets_failed_to_insert}')
    else:
        #Run stored procs to tie up loose ends:
        
        #change missing values replaced by 0 to NULL in {table}
        try:
            print('Here, a stored procedure would be executed to null the zero values that were just inserted.')
        except:
            print('Problem running procedure, please execute in SSMS')

        #update IDs in identity tables
        try:
            print('And here, a stored procedure is run that aggregates the inserted data, populates a series of tables with that data, and assigns primary keys.')
        except:
            print('Problem running procedure, please execute in SSMS')
            

print("""

Thank you for using Armadillo Routesheet ETL!


           .::7777::-.
          /:'////' `::>/|/ 
        .',  ||||   `/( e\\          
    -==~-'`-Xm````-mr' `-_\\ 
""")
    
            
if __name__ == '__main__':
    main()
        
