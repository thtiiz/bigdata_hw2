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