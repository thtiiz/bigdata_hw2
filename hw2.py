from pyhive import hive
from db import init_db, insert_data
from sqlalchemy import create_engine

host_name = "10.3.133.187"
port = 10000  #default is 10000
user = "root" # user name mysql 
password = "root" # pass mysql
database="default"

def hiveconnection(host_name, port, user,password, database):
    conn = hive.Connection(host=host_name, port=port, username=user,password=password, database=database, auth='CUSTOM')
    return conn

def query(cur):
    cur.execute('select name  from demo2 return limit 2')
    result = cur.fetchall()
    print(result)
# Call above function

def main():
    conn = hiveconnection(host_name, port, user,password, database)
    cur = conn.cursor()
    # create table
    init_db(cur, 'init_db.sql')
    # insert data to table
    insert_data(create_engine(f'hive://{user}@{host_name}:{port}/'), 'Coffee_Chain.csv', 'hw2')

if __name__ == "__main__":
    main()