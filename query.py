class Query:

    def __init__(self, cur, table_name):
        self.cur = cur
        self.table_name = table_name

    def top_5_profit(self):
        self.cur.execute(f'''SELECT state, SUM(profit) as total_profit
            FROM {self.table_name}
            GROUP BY state
            ORDER BY total_profit DESC
            LIMIT 5
        ''')
        result = self.cur.fetchall()
        return result

    def max_expense_per_profit(self):
        self.cur.execute(f'''SELECT state, (SUM(total_expenses) / SUM(profit)) as expense_per_profit
            FROM {self.table_name}
            GROUP BY state
            ORDER BY expense_per_profit DESC
            LIMIT 1
        ''')
        result = self.cur.fetchone()
        return result

    def top_3_most_popular(self):
        self.cur.execute(f'''SELECT product, COUNT(product) as amount
            FROM {self.table_name}
            GROUP BY product
            ORDER BY amount DESC
            LIMIT 3
        ''')
        result = self.cur.fetchall()
        return result

    def product_lines_sales_more_margin(self):
        self.cur.execute(f'''SELECT product_line
            FROM (
                SELECT product_line, SUM(sales) as total_sales, SUM(margin) as total_margin
                FROM {self.table_name}
                GROUP BY product_line
                HAVING total_sales > total_margin
            ) s
        ''')
        result = self.cur.fetchall()
        return result

    def different_profit(self):
        month = 12 # december
        self.cur.execute(f'''SELECT (
                SELECT SUM(profit) as total_profit
                FROM {self.table_name}
                WHERE YEAR(date_) = 2012 AND MONTH(date_) = {month}
                GROUP BY YEAR(date_)
            ) - (
                SELECT SUM(profit) as total_profit
                FROM {self.table_name}
                WHERE YEAR(date_) = 2013 AND MONTH(date_) = {month}
                GROUP BY YEAR(date_)
            )
        ''')
        result = self.cur.fetchone()
        return result