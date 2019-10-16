"""Load data i94_db database Fact and Dimentions tables
This script is used to process and load data into i94_db Database 
tables. 

This file can also be imported as a module and contains the following
functions:

    * process_song_file - Processing Song file and load data 
    * process_log_file - Processing Song file and load data
    * process_data - IIterating all files with in a directory
    * main - the main function of the script
"""

# Importing system libraries
import os
import glob
import psycopg2
import pandas as pd
import numpy as np
import sys

# Importing user defined libraries
from sql_queries import *
import config

def process_i94_code_file(cur, filename, code_type):
    """Processing song data file and loading data into Dimensions. 
    
    Parameters
    ___________
        cur : psycopg2 cursor object
            Cursor object for sparkifydb
        filepath : string
            Path of the file to be loaded
        code_type : string
            Code type for specific table load  
    Returns
    ___________
        None
    """
    
    # open I94 Country Code file
    df = pd.read_csv(filename, sep='=', quotechar="'", skipinitialspace = True, header=None)
    
    # Insert I94 coutry code
    code_df = df[[0,1]]
    for i, row in code_df.iterrows():
        cur.execute(code_type, row)

def process_temperature_file(cur, filename, code_type):
    """Processing I94 data file and loading data into Dimensions. 
    
    Parameters
    ___________
        cur : psycopg2 cursor object
            Cursor object for sparkifydb
        filepath : string
            Path of the file to be loaded
        code_type : string
            Code type for specific table load
    Returns
    ___________
        None
    """

    # Reading SAS file
    df = pd.read_csv(filename)
    df = df[df['Country']=="United States"]
    df = df.head(20000)
    # Populating Temperature data
    temperature_data = df[['dt','AverageTemperature','AverageTemperatureUncertainty','City','Country',\
                         'Latitude','Longitude']]
    for i, row in temperature_data.iterrows():
        cur.execute(code_type, row)
        
def process_i94_file(cur, filename, code_type):
    """Processing I94 data file and loading data into Dimensions. 
    
    Parameters
    ___________
        cur : psycopg2 cursor object
            Cursor object for sparkifydb
        filepath : string
            Path of the file to be loaded
        code_type : string
            Code type for specific table load
    Returns
    ___________
        None
    """

    # Reading SAS file
    df = pd.read_sas(filename, 'sas7bdat', encoding="ISO-8859-1")
    df = df.replace('\x00', '', regex=True)
    df = df.replace(np.NaN, '', regex=True)
    few_data = df.head(2000)
    # Populating Time Dimension
    I94_data = few_data[['cicid','i94yr','i94mon','i94cit','i94res','i94port',\
                   'arrdate','i94mode','i94addr','depdate','i94bir','i94visa',\
                   'count','dtadfile','visapost','occup','entdepa','entdepd', \
                   'entdepu','matflag','biryear','dtaddto','gender','insnum',\
                   'airline','admnum','fltno','visatype']]
    for i, row in I94_data.iterrows():
        cur.execute(code_type, row)

def valid_data(cur,conn):
    """Creating tables in Database. 
    
    Parameters 
    ___________
        cur : psycopg2 cursor object
            Cursor object for DB
        conn : psycopg2 connection object
            Connection object for DB
    
    Returns 
    ___________
        data_validated_flag : Y/N
    """
    
    for query in clean_table_queries:
        cur.execute(query)
        conn.commit()
        
def populate_tables(cur, conn):
    """Creating tables in Database. 
    
    Parameters 
    ___________
        cur : psycopg2 cursor object
            Cursor object for DB
        conn : psycopg2 connection object
            Connection object for DB
    
    Returns 
    ___________
        None
    """
    
    for query in populate_table_queries:
        cur.execute(query)
        conn.commit()

def validate_data(cur, conn, table_name, no_rec):
    """Validating data in tables  
    
    Parameters 
    ___________
        cur : psycopg2 cursor object
            Cursor object for DB
        conn : psycopg2 connection object
            Connection object for DB
        table name : Table that needs to be validates
        no_rec : Number of minimum record existing in table 
    
    Returns 
    ___________
        
    """
    
    sql_query = "SELECT count(*) FROM " + table_name 
    cur.execute(sql_query)
    row = cur.fetchall()
    print(row[0])
    if row[0] >= no_rec :
        return Y
    else:
        return N
    
def main():
    
    # creating sparkify DB connection
    conn = psycopg2.connect("host=127.0.0.1 dbname=i94_db user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # Processing counry code data and inserting into Dimension
    process_i94_code_file(cur, 'I94_country_code.txt', I94_country_desc_table_insert)
    
    # Processing Address Description data and inserting into Dimension
    process_i94_code_file(cur, 'I94_address_description.txt', I94_address_desc_table_insert)
    
    # Processing Airport Description data and inserting into Dimension
    process_i94_code_file(cur, 'I94_airport_description.txt', I94_airport_desc_table_insert)
    
    # Processing travel modes Description data and inserting into Dimension
    process_i94_code_file(cur, 'I94_travel_modes.txt', I94_travel_mode_table_insert)
    
    # Processing I94 Immigration data and inserting into Dimension
    process_i94_file(cur, config.sas_file_path + 'i94_apr16_sub.sas7bdat', I94_immigration_stg_table_insert)
    
    # Processing I94 Immigration data and inserting into Dimension
    process_temperature_file(cur, 'GlobalLandTemperaturesByCity.csv',\
                             city_temperature_table_insert)
    
    # Populate tables
    try:
        valid_data(cur, conn)
    except Exception as e:
        print("Error while validating data; error message " + str(e))
        cur.close()
        conn.close()
        sys.exit(1)
        
    # Populate tables
    try:
        populate_tables(cur, conn)
    except Exception as e:
        print("Error while populating tables; error message " + str(e))
        cur.close()
        conn.close()
        sys.exit(1)
        
    # Validating Data
    try:
        validated = validate_data(cur, conn, "I94_immigration_data", 1)
        
        if validated =='N':
            print("Data validation Failed")
        else:
            print("Data validation Passed")
        
    except Exception as e:
        print("Error while validating tables; error message " + str(e))
        cur.close()
        conn.close()
        sys.exit(1)   
        
    # closing the connection
    conn.close()

if __name__ == "__main__":
    main()