import pandas as pd
import mysql.connector as msql
from mysql.connector import Error

def main():

    # Create a database "customer" before running this method.

    df = pd.read_csv("data/customer.csv", index_col=False, delimiter=",")

    try:
        conn = msql.connect(host='localhost', database='customer', user='root', password='rootpassword')
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("You're connected to database: ", record)
            cursor.execute('DROP TABLE IF EXISTS customer.customer_info;')
            cursor.execute("CREATE TABLE customer.customer_info(first_name varchar(255),last_name varchar(255),company_name varchar(255),address varchar(255),city varchar(255),state varchar(255),zip int,phone1 varchar(255),phone2 varchar(255),email varchar(255),web varchar(255))")
            for i, row in df.iterrows():
                cursor.execute("INSERT INTO customer.customer_info VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", tuple(row))
                print("Record inserted")
                conn.commit()
    except Error as e:
        print("Error while connecting to MySQL", e)


if __name__ == "__main__":
    main()

