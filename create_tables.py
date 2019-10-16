""" Initialising sparkifydb Database
This script is used to intialise i94_db Database. It first drop 
and recreate the i94_db and then create required facts and 
dimensions.

This file can also be imported as a module and contains the following
functions:

    * create_database - Creating Database
    * drop_tables - Dropping Tables
    * drop_tmp_tables - Dropping Temporary Tables
    * create_tables - Creating Tables
    * create_tmp_tables - Dropping Temporary Tabls
    * main - the main function of the script
"""

# Importing system libraries
import psycopg2
import sys

# Importing user defined libraries
from sql_queries import *


def create_database():
    """Creating i94_db Database.If DB already exist then drop & recreate. 
    
    Parameters
    ___________
    None 
  
    Returns
    ___________
        cur : psycopg2 cursor object
            Cursor object for sparkifydb
        conn : psycopg2 connection object
            Connection object for sparkifydb
    """
    
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS i94_db")
    cur.execute("CREATE DATABASE i94_db WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=i94_db user=student password=student")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """Dropping tables from Database. 
    
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
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def drop_tmp_tables(cur, conn):
    """Dropping temporary tables from Database. 
    
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
    
    for query in drop_tmp_table_queries:
        cur.execute(query)
        conn.commit()
        
def create_table(cur, conn):
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
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def create_tmp_tables(cur, conn):
    """Creating temporary tables in Database. 
    
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
    
    for query in create_tmp_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    
    # Creating sparkifydb Database
    try:
        cur, conn = create_database()
    except Exception as e:
        print("Error while creating DB connection; error message " + str(e))
        cur.close()
        conn.close()
        sys.exit(1)
    
    # Dropping existing tables
    try:
        drop_tables(cur, conn)
    except Exception as e:
        print("Error while Dropping tables; error message " + str(e))
        cur.close()
        conn.close()
        sys.exit(1)
    
    # Creating all tables
    try:
        create_table(cur, conn)
    except Exception as e:
        print("Error while creating tables; error message " + str(e))
        cur.close()
        conn.close()
        sys.exit(1)
        
    print("Successfully Initialised Database")
    
    # Closing the Connection to Database 
    conn.close()


if __name__ == "__main__":
    main()