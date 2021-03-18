#!/usr/bin/env python
# -*- coding: utf-8 -*-


import psycopg2 as pg

# connection authentication:
# dbname = 'sparkifydb',
# user = 'student',
# password = 'student',
# host = '127.0.0.1'
# port = '5432'  # default

conn_string = "host='127.0.0.1' dbname='sparkifydb' user='student' password='student'"


# connect & open database in postgres
class ConnectPostgres:
    def __init__(self):
        conn = pg.connect(conn_string)

        self.conn = conn
        self.cur = conn.cursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cur.close()

        self.conn.close()

    # inserts dataframe data to table using insert_query
    def execute_insert(self, insert_query, df):
        try:
            self.cur.execute(insert_query, df)
            self.conn.commit()
        except pg.InternalError as e:
            print(f'duplicate key in dimension table, ignoring row: \n {df.to_dict()}')
            return False

    # selects columns from table using select_query
    def execute_select(self, select_query, cols):
        self.cur.execute(select_query, cols)
        return self.cur.fetchone()

    # inserts data to table columns
    def execute_copy(self, file, table_name, cols):
        self.cur.copy_from(file, table_name, columns=cols)
        self.conn.commit()
