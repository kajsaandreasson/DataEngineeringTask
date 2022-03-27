
from multiprocessing import connection
import sqlite3
import urllib.request
from sqlite3 import Error
import csv
from io import StringIO
import json



def load_data(URL):
   file = urllib.request.urlopen(URL)
   return file
   


def create_connection(dataBase_path):
    """ create a database connection to a SQLite database """
    connection = None
    try:
        connection = sqlite3.connect(dataBase_path)  # sqlite3.connect(':memory:') #  
        print(sqlite3.version)
    except Error as e:
        print(e)
    
    return connection


def create_table(connection, create_table_query):

   try:
      c = connection.cursor()
      table = c.execute(create_table_query)
      return table

   except Error as e:
      print(e)
   


def create_loan(connection, loan):

   sql = ''' INSERT INTO loans(id,
                              user_id,
                              loan_timestamp,
                              loan_amount,
                              loan_purpose,
                              outcome,
                              interest,
                              webvisit_id)
            VALUES(?,?,?,?,?,?,?,?) '''

   cursor = connection.cursor()
   try:
      cursor.execute(sql, loan)
      connection.commit()

   except sqlite3.IntegrityError as error:
      print(error)


def create_visit(connection, visit):

   sql = ''' INSERT INTO visits(
                           visit_id,
                           id,
                           visit_timestamp,
                           referrer,
                           campaing_name)
            VALUES(?,?,?,?,?) '''

   cursor = connection.cursor()
   try:
      cursor.execute(sql, visit)
      connection.commit()

   except sqlite3.IntegrityError as error:
      print(error)


def create_customer(connection, customer):

   sql = ''' INSERT INTO customers(
                           user_id,
                           name,
                           ssn,
                           birthday,
                           gender,
                           city,
                           zip_code)
            VALUES(?,?,?,?,?,?,?) '''

   cursor = connection.cursor()
   try:
      cursor.execute(sql, customer)
      connection.commit()

   except sqlite3.IntegrityError as error:
      print(error)
      return 


def load_loan_data(connection, URL_LOANS):
   

   for year in ['17', '18', '19']:
      for month in range(1, 12):
         URL = URL_LOANS.format(year, str(month))

         try:
            data = load_data(URL)
         except urllib.error.HTTPError:
            continue
         
         first_line = True
         for line in data:

            if first_line:
               first_line = False
               continue

            loan = line.decode("utf-8").strip().rstrip(",").lstrip(",").split(",")[1:]

            if len(loan) >= 8:
               loan = loan[:8]
               loan[7] = loan[7].lstrip('"(')

            while len(loan) < 8:
               loan.append(None)

            create_loan(connection, loan)


def load_visits_data(connection, URL_VISITS):
   
   data = load_data(URL_VISITS)
   
   first_line = True
   for line in data:

      if first_line:
         first_line = False
         continue

      visit = line.decode("utf-8").strip().rstrip(",").lstrip(",").split(",")
      create_visit(connection, visit)


def load_customer_data(connection, URL_CUSTOMERS):
   
   data = load_data(URL_CUSTOMERS)

   first_line = True
   for line in data:

      if first_line:
         first_line = False
         continue
      
      customer = line.decode("utf-8")
      JSON_object = json.loads(customer)
      cust_data = list(JSON_object.values())
      create_customer(connection, cust_data)


         
if __name__ == '__main__':
   dataBase_path = r"/Users/kajsaandreasson/DataEngineeringTask/dataBase/loanDataBase2.db"
   URL_VISITS = "http://rocker-data-engineering-task.storage.googleapis.com/data/visits.csv"
   URL_LOANS = "http://rocker-data-engineering-task.storage.googleapis.com/data/loan-20{}-{}.csv"
   URL_CUSTOMERS = "http://rocker-data-engineering-task.storage.googleapis.com/data/customers.json"

   sql_create_loan_table = """ CREATE TABLE IF NOT EXISTS loans (
                                        id integer PRIMARY KEY,
                                        user_id integer,
                                        loan_timestamp integer,
                                        loan_amount integer,
                                        loan_purpose text,
                                        outcome text,
                                        interest real,
                                        webvisit_id text
                                    ); """

   sql_create_visits_table = """ CREATE TABLE IF NOT EXISTS visits (
                                       visit_id integer PRIMARY KEY,
                                       id long,
                                       visit_timestamp integer,
                                       referrer text,
                                       campaing_name text
                                    ); """

   sql_create_customers_table = """ CREATE TABLE IF NOT EXISTS customers (
                                       user_id long PRIMARY KEY,
                                       name text,
                                       ssn text,
                                       birthday date,
                                       gender text,
                                       city text,
                                       zip_code text
                                    ); """
   
   connection = create_connection(dataBase_path)

   if connection:
      loan_table = create_table(connection, sql_create_loan_table)
      visits_table = create_table(connection, sql_create_visits_table)
      customer_table = create_table(connection, sql_create_customers_table)


   with connection:
      #load_loan_data(connection, URL_LOANS)
      #load_visits_data(connection, URL_VISITS)
      load_customer_data(connection, URL_CUSTOMERS)

      
      

      
