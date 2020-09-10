import re
import pandas as pd
from datetime import datetime

def init_db(cur, file):
    # Open and read the file as a single buffer
    fd = open(file, 'r')
    sqlFile = fd.read()
    fd.close()

    # all SQL commands (split on ';')
    sqlCommands = sqlFile.split(';')

    # clear empty string
    sqlCommands = list(filter(None, sqlCommands))

    for command in sqlCommands:
        cur.execute(command)

def insert_data(cur, path, table_name):
    df = pd.read_csv(path)
    money_columns = ['Budget COGS',	'Budget Margin', 'Budget Profit', 'Budget Sales',
    'COGS', 'Inventory', 'Margin', 'Marketing', 'Profit', 'Sales', 'Total Expenses']

    # apply clean_money function and cast to int
    for money_column in money_columns:
        df[money_column] = df[money_column].apply(clean_money).astype(int)
    
    # apply clean date column
    date_column = 'Date'
    df[date_column] = df[date_column].apply(lambda d: datetime.strptime(d, '%d/%m/%Y'))

    # rename
    df = rename_df(df)

    # insert to db
    df.to_sql(name=table_name, if_exists='append', con=cur, index=False, method='multi')




def rename_df(df):
    new_name = {
        'Area Code': 'area_code',
        'Date': 'date_',
        'Territory': 'territory',
        'Territory Size': 'territory_size',
        'Product': 'product',
        'Product Line': 'product_line',
        'Product Type': 'product_type',
        'State': 'state',
        'Type': 'type',
        'Budget COGS': 'budget_cogs',
        'Budget Margin': 'budget_margin',
        'Budget Profit': 'budget_profit',
        'Budget Sales': 'budget_sales',
        'COGS': 'cogs',
        'Inventory': 'inventory',
        'Margin': 'margin',
        'Marketing': 'marketing',
        'Profit': 'profit',
        'Sales': 'sales',
        'Total Expenses': 'total_expenses'
    }
    return df.rename(columns = new_name)

def clean_money(money):
    r = re.search(r'(\(?)?\$?([\d,]+)(\)?)', str(money))

    # not match anything
    if(not r):
        return money

    is_negative = r.group(1)
    value = r.group(2).replace(',', '') # remove comma
    if(is_negative):
        return '-' + value
    else:
        return value