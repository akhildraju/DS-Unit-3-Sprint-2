import numpy as np
import pandas as pd
import os
import json
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.extensions import register_adapter, AsIs
from dotenv import load_dotenv

psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)


class ImportTitanicData:

    df = None

    DB_NAME = None
    DB_USER = None
    DB_PW = None
    DB_HOST = None

    connection = None

    def __init__(self):
        self.df = pd.read_csv("https://raw.githubusercontent.com/LambdaSchool/DS-Unit-3-Sprint-2-SQL-and-Databases/master/module2-sql-for-analysis/titanic.csv")
        print(self.df.head())
        print(self.df.dtypes)

        load_dotenv() #> loads contents of the .env file into the script's environment
        self.DB_NAME = os.getenv("DB_NAME", default="OOPS")
        self.DB_USER = os.getenv("DB_USER", default="OOPS")
        self.DB_PW = os.getenv("DB_PW", default="OOPS")
        self.DB_HOST = os.getenv("DB_HOST", default="OOPS")
        self.connection = psycopg2.connect(dbname=self.DB_NAME, user=self.DB_USER, password=self.DB_PW, host=self.DB_HOST)
        print(self.connection)

    def create_table(self):
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS titanic_data (
                Survived integer,
                Pclass integer,
                Name varchar(100),
                Sex varchar(6),
                Age decimal,
                SiblingsSpouses integer,
                ParentsChildren integer,
                Fare decimal
            );"""

        cursor = self.connection.cursor()
        cursor.execute(create_table_sql)
        self.connection.commit()
        cursor.close()

    def insert_into_pg(self):

        insertion_query = f"INSERT INTO titanic_data (Survived, Pclass, Name, Sex, Age, SiblingsSpouses, ParentsChildren, Fare) VALUES %s"

        records = self.df.to_records(index=False)
        result = list(records)
        # print (result)
        
        cursor = self.connection.cursor()
        execute_values(cursor, insertion_query, result)         

        self.connection.commit() 
        cursor.close()


def main():
    ins = ImportTitanicData()
    ins.create_table()
    ins.insert_into_pg()

if __name__ == "__main__":
    main()

