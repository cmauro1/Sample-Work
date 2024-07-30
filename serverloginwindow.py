# -*- coding: utf-8 -*-
"""
Created on Wed Sep 13 15:11:43 2023

@author: cjmauro
"""

import pyodbc as db
import tkinter as tk
from tkinter.ttk import *
from sqldatahandler import SqlDataHandler


class ServerLoginWindow():
       
       def __init__(self, sqldatahandler):
              
              #required to connect to server
              self.sqldatahandler = sqldatahandler
       
              #root frame widget
              self.root = tk.Tk()
              self.frame = Frame(self.root, padding=10)
              self.frame.grid()
       
              #server components
              self.server_input = tk.StringVar(self.root)
              self.server_label = Label(self.frame, text='Server').grid(column=0, row=1)
              self.server_entry = Entry(self.frame, textvariable=self.server_input, width=20).grid(column=1, row=1)
           
              #database components
              self.database_input = tk.StringVar(self.root)
              self.database_label = Label(self.frame, text='Database').grid(column=0, row=2)
              self.database_entry = Entry(self.frame, textvariable=self.database_input, width=20).grid(column=1, row=2)
           
              #username components
              self.username_input = tk.StringVar(self.root)
              self.username_label = Label(self.frame, text='Username').grid(column=0, row=3)
              self.username_entry = Entry(self.frame, textvariable=self.username_input, width=20).grid(column=1, row=3)
       
              #password components
              self.password_input = tk.StringVar(self.root)
              self.password_label = Label(self.frame, text='Password').grid(column=0, row=4)
              self.password_entry = Entry(self.frame, show='*', textvariable=self.password_input, width=20).grid(column=1, row=4)

              #submit button components
              self.submit_button = Button(self.frame, text='Submit', command=self.submit_credentials).grid(column=1, row=6)
              

       def submit_credentials(self):
              connection_string = self.parse_connection_string_from_input()
              sql_connection = db.connect(connection_string)
              self.sqldatahandler.set_sql_connector(sql_connection)
              self.root.destroy()
       
       
       def parse_connection_string_from_input(self):
              DRIVER = '{ODBC Driver 18 for SQL Server}'
              server = self.server_input.get()
              database = self.database_input.get()
              username = self.username_input.get()
              password = self.password_input.get()
              connection_string = f'DRIVER={DRIVER};SERVER={server};DATABASE={database};UID={username};PWD={password};Trusted_Connection=yes;Encrypt=no'
              return connection_string
       
       
       def mainloop(self):
              self.root.mainloop()       

