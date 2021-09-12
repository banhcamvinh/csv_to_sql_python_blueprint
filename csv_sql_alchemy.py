import pandas as pd
import pyodbc
import sqlalchemy
from sqlalchemy import event
import time
import urllib

data = pd.read_csv('100000jobpost.csv')   
data_df = pd.DataFrame(data)

def connectDB_pyodbc():
    print("Connecting....")
    try:
        conn = pyodbc.connect('Driver={SQL Server};'
                            'Server=database-sql-1.ctlrg00ritgt.us-east-2.rds.amazonaws.com;'
                            'Database=test;'
                            'UID=admin;'
                            'PWD=12345678;')
        # conn = pyodbc.connect('Driver={SQL Server};'
        #                     'Server=LAPTOP-N94DSRJ8\VINHSEVER1;'
        #                     'Database=test;'
        #                     'UID=sa;'
        #                     'PWD=123;')
        print("Connect success")
    except Exception as Ex:
        print("Connect failed")
        print(Ex)
    return conn

def connectDB_sqlalchemy():
    print("Connecting server . . .")
    params = urllib.parse.quote_plus("DRIVER=ODBC Driver 11 for SQL Server;"
                                    "SERVER=database-sql-1.ctlrg00ritgt.us-east-2.rds.amazonaws.com;"
                                    "DATABASE=test;"
                                    "UID=admin;"
                                    "PWD=12345678")
    # params = urllib.parse.quote_plus("DRIVER=ODBC Driver 11 for SQL Server;"
    #                                 "SERVER=LAPTOP-N94DSRJ8\VINHSEVER1;"
    #                                 "DATABASE=test;"
    #                                 "UID=sa;"
    #                                 "PWD=123")
    engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
    @event.listens_for(engine, "before_cursor_execute")
    def receive_before_cursor_execute(
        conn, cursor, statement, params, context, executemany
            ):
                if executemany:
                    cursor.fast_executemany = True
    try:
        conn = engine.connect()
        print("Connected")
    except Exception as Ex:
        print("Connect failed")
        print(Ex)
    return conn

def create_sql_table(table_name,columns_name,columns_type,db_con):
    create_table_query = 'create table '+ table_name +'( '
    index = 0
    while index < len(columns_name)-1:
        create_table_query += columns_name[index] + ' ' + columns_type[index] + ','
        index += 1
    create_table_query += columns_name[index] + ' ' + columns_type[index] + ');'
    cursor = db_con.cursor()
    try:
        cursor.execute(create_table_query)
        db_con.commit()
        print('Create table success')
    except Exception as ex:
        print("Create table failed")
        print(ex)

def return_df_to_sql_columns_name(df):
    return list(df)

table_name = 'csv_to_sql'
columns_name = return_df_to_sql_columns_name(data_df)
columns_type = ['int primary key','nvarchar(max)','nvarchar(max)','nvarchar(max)','nvarchar(max)','date','nvarchar(max)']

db_con = connectDB_pyodbc()
create_sql_table(table_name,columns_name,columns_type,db_con)
db_con.close()

db_con = connectDB_sqlalchemy()
print("Start to insert")
data_df.to_sql("csv_to_sql",db_con,index=False,if_exists="append",schema="dbo")
print("finish")


