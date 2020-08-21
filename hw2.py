from pyhive import hive
from db import init_db, insert_data
from sqlalchemy import create_engine
from query import Query

host_name = "10.3.133.187"
port = 10000  #default is 10000
user = "root" # user name mysql 
password = "root" # pass mysql
database="default"

def hiveconnection(host_name, port, user,password, database):
    conn = hive.Connection(host=host_name, port=port, username=user,password=password, database=database, auth='CUSTOM')
    return conn

def main():
    conn = hiveconnection(host_name, port, user,password, database)
    cur = conn.cursor()
    # create table
    # init_db(cur, 'init_db.sql')
    
    # insert data to table
    table_name = 'hw2'
    file =  'Coffee_Chain.csv'
    insert_data(create_engine(f'hive://{user}@{host_name}:{port}/'), file, table_name)

    # new query instance
    q = Query(cur, table_name)

    # top_5_profit = q.top_5_profit()
    # print('top 5 profit: ', top_5_profit)

    # max_expense_per_profit = q.max_expense_per_profit()
    # print('max expense per profit: ', max_expense_per_profit)

    # top_3_most_popular = q.top_3_most_popular()
    # print('top 3 most popular product: ', top_3_most_popular)

    # product_lines_sales_more_margin = q.product_lines_sales_more_margin()
    # print('product lines that have sales more than margin: ', product_lines_sales_more_margin)

    # different_profit = q.different_profit()
    # print('', different_profit)



if __name__ == "__main__":
    main()