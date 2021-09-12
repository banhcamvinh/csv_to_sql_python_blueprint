import pandas as pd
import pyodbc
import time

data = pd.read_csv('100000jobpost.csv')   
data_df = pd.DataFrame(data)

def connectDB():
    print("Connecting....")
    try:
        # conn = pyodbc.connect('Driver={SQL Server};'
        #                     'Server=database-sql-1.ctlrg00ritgt.us-east-2.rds.amazonaws.com;'
        #                     'Database=test;'
        #                     'UID=admin;'
        #                     'PWD=12345678;')
        conn = pyodbc.connect('Driver={SQL Server};'
                            'Server=LAPTOP-N94DSRJ8\VINHSEVER1;'
                            'Database=test;'
                            'UID=sa;'
                            'PWD=123;')
        print("Connect success")
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

db_con = connectDB()
create_sql_table(table_name,columns_name,columns_type,db_con)

print("Start to insert")

init_insert_query = 'insert into {} values(?,?,?,?,?,?,?)'.format(table_name)

row_index = 0
for row in data_df.itertuples(index= False):
    # values_list = []
    # col_name_index = 1
    # values_list.append(int(row[data_df.columns.get_loc(columns_name[0])]))
    # while col_name_index < len(columns_name):
    #     value = row[data_df.columns.get_loc(columns_name[col_name_index])]
    #     values_list.append(value)
    #     col_name_index += 1
    
    try:
        cursor = db_con.cursor()
        # cursor.execute(init_insert_query,values_list)
        cursor.execute(init_insert_query,[int(row.ref_id),str(row.title),str(row.description),str(row.benefit),str(row.contact_name),str(row.created_at),str(row.skill_requirement),])
        db_con.commit()
    except Exception as ex:
        print("Insert failed at "+ str(row_index))
        print(ex)
    
    row_index += 1
    if row_index % 100 == 0:
        print(str(row_index) + '============ id '+str(row.ref_id))
        # time.sleep(2)
    







