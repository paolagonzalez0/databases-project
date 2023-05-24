import sqlite3
import pandas as pd
from traceback import print_exc as pe
import numpy as np
sqlite3.register_adapter(np.int64, lambda x: int(x))

from glob import glob
import os

def Connect():
    conn = sqlite3.connect('../databases/store.db')
    curs = conn.cursor()
    curs.execute("PRAGMA foreign_keys=ON;")
    return conn, curs

def RunAction(sql, params=None):
    conn, curs = Connect()
    if params is not None:
        curs.execute(sql, params)
    else:
        curs.execute(sql)
    conn.close()
    return

def RunQuery(sql, params=None):
    conn, curs = Connect()
    if params is not None:
        results = pd.read_sql(sql, conn, params=params)
    else:
        results = pd.read_sql(sql, conn)
    conn.close()
    return results

def RebuildTables():
    RunAction("DROP TABLE IF EXISTS tOrderDetail;")
    
    RunAction("DROP TABLE IF EXISTS tOrder;")
    
    RunAction("DROP TABLE IF EXISTS tProd;")
    
    RunAction("DROP TABLE IF EXISTS tCust;")
    
    RunAction("DROP TABLE IF EXISTS tZip;")

    RunAction("DROP TABLE IF EXISTS tState;")

    
    sql = """
    CREATE TABLE tCust (
        cust_id INTEGER PRIMARY KEY AUTOINCREMENT,
        first TEXT NOT NULL,
        last TEXT NOT NULL,
        addr TEXT NOT NULL,
        zip TEXT NOT NULL REFERENCES tZip(zip)
    );"""
    RunAction(sql)
    
    sql = """
    CREATE TABLE tZip (
        zip TEXT PRIMARY KEY CHECK(length(zip)==5),
        city TEXT NOT NULL,
        st TEXT NOT NULL REFERENCES tState(st)
    );"""
    RunAction(sql)
    
    sql = """
    CREATE TABLE tState (
        st TEXT PRIMARY KEY CHECK(length(st)==2),
        state TEXT NOT NULL
    );"""
    RunAction(sql)
    
    sql = """
    CREATE TABLE tOrder (
        order_id INTEGER PRIMARY KEY AUTOINCREMENT,
        cust_id INTEGER NOT NULL REFERENCES tCust(cust_id),
        date TEXT NOT NULL CHECK(date LIKE '____-__-__')
    );"""
    RunAction(sql)
    
    sql = """
    CREATE TABLE tOrderDetail (
        order_id INTEGER NOT NULL REFERENCES tOrder(order_id),
        prod_id INTEGER NOT NULL REFERENCES tProd(prod_id),
        qty INTEGER NOT NULL CHECK(qty>0),
        PRIMARY KEY (order_id, prod_id)
    );"""
    RunAction(sql)
    
    sql = """
    CREATE TABLE tProd (
        prod_id INTEGER PRIMARY KEY,
        prod_desc TEXT NOT NULL,
        unit_price REAL NOT NULL
    );"""
    RunAction(sql)
    
    return 

def LoadTable(df, table_name):
    conn, curs = Connect()
    sql = "INSERT INTO " + table_name + ' (' + ','.join(list(df.columns)) + \
      ') VALUES (:' + ',:'.join(list(df.columns)) + ');'
    try:
        for i, row in enumerate(df.to_dict(orient='records')):
            curs.execute(sql, row)
    except:
        print(i)
        print(row)
        pe()
        conn.rollback()
        conn.close()
        return False
    
    conn.commit()
    conn.close()
    return True

def LoadLookups():
    tProd = pd.read_csv('data/lookups/prods.csv')
    tState = pd.read_csv('data/lookups/states.csv')
    tZip = pd.read_csv('data/lookups/zips.csv', dtype={'zip':str})

    tProd.columns = ['prod_id', 'prod_desc', 'unit_price']
    tZip.columns = ['zip', 'city', 'st']

    LoadTable(tProd, 'tProd')
    LoadTable(tState, 'tState')
    LoadTable(tZip, 'tZip')
    
    return

def RebuildDB():
    RebuildTables()
    LoadLookups()
    return

PATH_DATA = 'data'
PATH_TO_LOAD = os.path.join(PATH_DATA,'sales_to_load')
PATH_LOADED = os.path.join(PATH_DATA,'sales_loaded')

SALES_PATTERN = 'Sales_*.csv'


# Check if we have any new data to load
def GetNewData():
    """ Check if there are any files named like Sales_*.csv in the sales_to_load folder
    and return the list of the file names"""
    file_names = glob(os.path.join(PATH_TO_LOAD,SALES_PATTERN))
    file_names.sort() #Make sure they are in order
    return file_names

# LoadNewFiles function loads any files in the sales_to_load folder into store.db
def LoadNewFiles():
    # Grab all the new files
    file_names = GetNewData()
    # Load each file one by one
    for file in file_names:
        # If file loaded succesfully, move it to the sales_loaded directory
        if LoadData(file):
            new_file = file.replace(PATH_TO_LOAD,PATH_LOADED)
            os.rename(file, new_file)
            print("File: " + file + " loaded successfully!")
        # If file did not load successfully, return False and print "Failed to load" message
        else:
            print("Failed to load: " + file)


# LoadData function a single sales file
def LoadData(file_name): 
    
    # GetCustomerID function will check if a cust_id exists, and if not, it will create a new record
    # So, function will always return a valid cust_id
    def GetCustomerID(row):
        # Check if person has a cust_id
        sql_check = """
            SELECT cust_id
            FROM tCust
            WHERE first = :first
            AND last = :last
            AND addr = :addr
            AND zip = :zip
            ;"""
        cust_id = pd.read_sql(sql_check, conn, params=row)
        #If cust_id does not exist for that customer, create it using INSERT 
        if len(cust_id) == 0:
            curs.execute("""INSERT INTO tCust (first,last,addr,zip) VALUES (:first,:last,:addr,:zip);""", row)
        # Grab the cust_id and add it to the dictionary
        cust_id = pd.read_sql(sql_check, conn, params=row).iloc[0][0]
        row['cust_id'] = cust_id
        # Return the cust_id
        return row['cust_id']
    
    # Function will check if an order_id exists, and if not, it will create a new record
    # So, function will always return a valid order_id
    def GetOrderID(row):
        # Check if order_id exists
        sql_check = """
            SELECT order_id
            FROM tOrder
            WHERE cust_id = :cust_id
            AND date = :date
            ;"""
        order_id = pd.read_sql(sql_check, conn, params=row)
        #If order_id does not exist, create it
        if len(order_id) == 0:
            curs.execute("INSERT INTO tOrder (cust_id,date) VALUES (:cust_id,:date)",row)
        # Grab the order_id and add it to the dictionary
        order_id = pd.read_sql(sql_check, conn, params=row).iloc[0][0]
        row['order_id'] = order_id
        # Return the order_id
        return order_id
    # Open file and intialize a connection to the database
    file = pd.read_csv(file_name, dtype={'zip':str})
    conn, curs = Connect()
    try:
        # Load data from file, line by line
        for row in file.to_dict(orient='records'):
            # Get the cust_id (or create a new one)
            cust_id = GetCustomerID(row)
            #Get the order_id (or create a new one)
            order_id = GetOrderID(row)
            # Insert the data into tOrderDetail
            curs.execute("""INSERT INTO tOrderDetail(order_id,prod_id,qty) VALUES (:order_id,:prod_id,:qty);""",row)
    except:
        # Print info about what went wrong, which file, what row, etc.
        pe()
        print(file)
        print(row)
        conn.rollback()
        conn.close()
        return False
    conn.commit()
    conn.close()
    return True

# Note: The functions below were written for a separate assignment.

def GetMailingList():
    sql = """
    WITH 
    AllProdsBoughtPerCust as
        (
        WITH 
        ProdsOverFive as
            (
            SELECT prod_id
            FROM tProd
            WHERE unit_price >= 5
            )
        SELECT cust_id, first, last, prod_id
        FROM tCust
        JOIN tOrder USING(cust_id)
        JOIN tOrderDetail USING(order_id)
        WHERE prod_id IN ProdsOverFive
        GROUP BY cust_id, prod_id
        ORDER BY cust_id, prod_id
    )
    SELECT cust_id, first, last
    FROM AllProdsBoughtPerCust
    GROUP BY cust_id
    HAVING count(cust_id) < (SELECT count(*)
                             FROM tProd
                             WHERE unit_price >= 5)
    ;"""
    mailing_list = RunQuery(sql)
    return mailing_list

def GetProductNeverPurchased(cust_id):
    sql = """
    WITH 
    NeverBought as
    (
        WITH 
        AllCombos as
        (
        SELECT cust_id, first, last, prod_id
        FROM (SELECT prod_id
              FROM tProd
              WHERE unit_price >= 5)
        CROSS JOIN tCust
        ORDER BY cust_id, prod_id
        ),
        CurrCombos as
        (
        SELECT cust_id, first, last, prod_id
        FROM tCust
        JOIN tOrder USING(cust_id)
        JOIN tOrderDetail USING(order_id)
        WHERE prod_id IN (SELECT prod_id
                          FROM tProd
                          WHERE unit_price >= 5)
        GROUP BY cust_id, prod_id
        ORDER BY cust_id, prod_id
        )
        SELECT *
        FROM AllCombos
        LEFT JOIN CurrCombos USING(cust_id,first,last,prod_id)
        JOIN tProd USING(prod_id)
        WHERE CurrCombos.first IS NULL
        ORDER BY unit_price DESC
    )
    SELECT *
    FROM NeverBought
    WHERE cust_id = :cust_id
    GROUP BY cust_id
    ;"""
    final_prod2 = RunQuery(sql, params=(cust_id,))
    return final_prod2

def GetRecentlyPurchasedProduct(cust_id):
    sql = """
    SELECT cust_id, first, last, prod_id, prod_desc, MAX(unit_price)
    FROM tOrderDetail
    JOIN tProd USING(prod_id)
    JOIN tOrder USING(order_id)
    JOIN tCust USING(cust_id)
    WHERE order_id = (SELECT MAX(order_id) as LatestOrder
                      FROM tOrder
                      WHERE cust_id = :cust_id)
    ;"""
    final_prod1 = RunQuery(sql, params=(cust_id,))
    return final_prod1

def MakeLetter(cust_id,first,last,prod1,prod2):
    letter = F"""
    Dear {first} {last},

    Thank you for being a valued customer of the Hardware Store.

    We hope you are enjoying your recent purchase of {prod1}.

    We see you haven't purchased any {prod2}, and we'd like to offer you a special deal! Please contact us for more info.

    Sincerely,
    The Hardware Store
    """
    file_name = F"CustomerLetter_{cust_id}.txt"

    with open(file_name, 'w') as f:
        f.write(letter)
    return file_name

def MakeAllLetters():  
    mailing_list = GetMailingList()
    for i in range(len(mailing_list)):
        cust_id = mailing_list['cust_id'][i]
        first = mailing_list['first'][i]
        last = mailing_list['last'][i]
        prod1 = GetRecentlyPurchasedProduct(cust_id)['prod_desc'][0]
        prod2 = GetProductNeverPurchased(cust_id)['prod_desc'][0]
        file_name = MakeLetter(cust_id,first,last,prod1,prod2)
        print(file_name + " successfully written.")