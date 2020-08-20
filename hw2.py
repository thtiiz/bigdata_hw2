from pyhive import hive
from db import init_db

host_name = "localhost"
port = 10000  #default is 10000
user = "" # user name mysql 
password = "" # pass mysql
database="default"

def hiveconnection(host_name, port, user,password, database):
    conn = hive.Connection(host=host_name, port=port, username=user,password=password, database=database, auth='CUSTOM')
    cur = conn.cursor()
    return cur

def query():
    cur.execute('select name  from demo2 return limit 2')
    result = cur.fetchall()
# Call above function

def main():
    # cur = hiveconnection(host_name, port, user,password, database)
    cur = False
    init_db(cur, 'init_db.sql')
    insert_data(cur, 'Coffee_Chain.csv', 'hw2')

if __name__ == "__main__":
    main()