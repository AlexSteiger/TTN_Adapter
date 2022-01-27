#!/usr/bin/python3

import psycopg2

connectionstring = """
	host=localhost 
	dbname=suppliers 
	user=postgres 
	password=postgres
"""

def select():

    """ insert multiple vendors into the vendors table  """
    sql = (
        """
        SELECT * FROM vendors
        """
)
    conn = None
    try:
        # connect to the PostgreSQL server
        conn = psycopg2.connect(connectionstring)
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql)
        print("The number of vendors: ", cur.rowcount)
        row =  cur.fetchone()

        while row is not None:
            print(row)
            row = cur.fetchone()

        # close communication with the PostgreSQL database server
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

if __name__ == '__main__':
    select()
