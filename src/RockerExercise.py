import sqlite3
import urllib.request
import csv
import json
from sqlite3 import Error
from multiprocessing import connection



def load_data(URL):
   """ Open URL """
   file = urllib.request.urlopen(URL)
   return file
   

def create_connection():
    """ Create a database connection to a SQLite database """
    connection = None
    try:
        connection = sqlite3.connect(':memory:')
    except Error as e:
        print(e)
    
    return connection


def create_table(connection, create_table_query):
   """ Create a table based on the instructions given in the parameter 'create_table_query' """

   try:
      c = connection.cursor()
      table = c.execute(create_table_query)
      return table

   except Error as e:
      print(e)
   


def create_loan(connection, loan):
   """ Create a loan instance in the loans-table in the database """

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
   """ Create a visit instance in the visits-table in the database """

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
   """ Create a customer instance in the customer-table in the database """

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


def load_loan_data(connection, URL_LOANS):
   """ Read the loan-data from all loan data-files, and insert all instances 
   into the loan-table in the database. """

   # Iterate over all possible loan-files, using the years and months to format the file path
   for year in ['17', '18', '19']:
      for month in range(1, 12):

         # Format URL with the current year and month
         URL = URL_LOANS.format(year, str(month))

         # Try tead the data from the formatted URL if the file exists
         try:
            data = load_data(URL)
         except urllib.error.HTTPError:
            continue
         
         # Iterate over all lines in the csv-data, format the line and insert in table in database
         first_line = True
         for line in data:

            # Skip first line with column names
            if first_line:
               first_line = False
               continue
            
            # Parsing the line
            loan = line.decode("utf-8").strip().rstrip(",").lstrip(",").split(",")[1:]

            # Remove excess elements in line
            if len(loan) >= 8:
               loan = loan[:8]
               loan[7] = loan[7].lstrip('"(')

            # Add empty elements to compensate for missing parameter values in line
            while len(loan) < 8:
               loan.append(None)

            create_loan(connection, loan)


def load_visits_data(connection, URL_VISITS):
   """ Read the visit-data the given URL, and insert all instances 
   into the visits-table in the database. """

   data = load_data(URL_VISITS)
   
   first_line = True
   for line in data:

      # Skip first line with column names
      if first_line:
         first_line = False
         continue
      
      # Parse line and insert instance into table
      visit = line.decode("utf-8").strip().rstrip(",").lstrip(",").split(",")
      create_visit(connection, visit)


def load_customer_data(connection, URL_CUSTOMERS):
   """ Read the customer-data the given URL, and insert all instances 
   into the customers-table in the database. """

   data = load_data(URL_CUSTOMERS)

   first_line = True
   for line in data:

      # Skip first line with column names
      if first_line:
         first_line = False
         continue
      
      # Parse line and insert instance into table
      customer = line.decode("utf-8")
      JSON_object = json.loads(customer)
      customer_data = list(JSON_object.values())
      create_customer(connection, customer_data)


def join_tables_and_write_to_file(connection, OUTPUT_DATA_PATH):
   """ Join loan, visit and customer tables and write to csv-file """

   query = ''' SELECT loans.id,
                     loans.user_id,
                     loan_timestamp,
                     loan_amount,
                     loan_purpose,
                     outcome,
                     interest,
                     webvisit_id,
                     customers.name as name,
                     customers.ssn as ssn,
                     customers.birthday as birthday,
                     customers.gender,
                     customers.city,
                     customers.zip_code,
                     visits.visit_timestamp,
                     visits.referrer,
                     visits.campaing_name

               FROM loans 
               LEFT JOIN customers ON customers.user_id = loans.user_id
               LEFT JOIN visits ON visits.id = loans.webvisit_id
               '''

   cursor = connection.cursor()
   cursor.execute(query)
   connection.commit()

   with open(OUTPUT_DATA_PATH, "w") as csv_file:
      csv_writer = csv.writer(csv_file)
      csv_writer.writerow([i[0] for i in cursor.description])
      csv_writer.writerows(cursor)


         
if __name__ == '__main__':

   URL_VISITS = "http://rocker-data-engineering-task.storage.googleapis.com/data/visits.csv"
   URL_LOANS = "http://rocker-data-engineering-task.storage.googleapis.com/data/loan-20{}-{}.csv"
   URL_CUSTOMERS = "http://rocker-data-engineering-task.storage.googleapis.com/data/customers.json"
   OUTPUT_DATA_PATH = "data/final_data.csv"

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
   
   connection = create_connection()

   if connection:
      loan_table = create_table(connection, sql_create_loan_table)
      visits_table = create_table(connection, sql_create_visits_table)
      customer_table = create_table(connection, sql_create_customers_table)

   with connection:
      load_loan_data(connection, URL_LOANS)
      load_visits_data(connection, URL_VISITS)
      load_customer_data(connection, URL_CUSTOMERS)
      join_tables_and_write_to_file(connection, OUTPUT_DATA_PATH)

      
      

      
