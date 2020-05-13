import os
import json
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from rpg_queries import RpgQueries

class RpgInsert:
    DB_NAME = None
    DB_USER = None
    DB_PW = None
    DB_HOST = None

    connection = None

    def __init__(self):
        load_dotenv() #> loads contents of the .env file into the script's environment
        self.DB_NAME = os.getenv("DB_NAME", default="OOPS")
        self.DB_USER = os.getenv("DB_USER", default="OOPS")
        self.DB_PW = os.getenv("DB_PW", default="OOPS")
        self.DB_HOST = os.getenv("DB_HOST", default="OOPS")
        self.connection = psycopg2.connect(dbname=self.DB_NAME, user=self.DB_USER, password=self.DB_PW, host=self.DB_HOST)
        print(self.connection)


    def create_table(self):
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS charactercreator_character (
                character_id integer PRIMARY KEY,
                name varchar(30) NOT NULL,
                level integer NOT NULL,
                exp integer NOT NULL,
                hp integer NOT NULL,
                strength integer NOT NULL,
                intelligence integer NOT NULL,
                dexterity integer NOT NULL,
                wisdom integer NOT NULL
            );"""

        cursor = self.connection.cursor()
        cursor.execute(create_table_sql)
        self.connection.commit()
        cursor.close()

    def insert_characters(self):

        insertion_query = f"INSERT INTO charactercreator_character (character_id, name, level, exp, hp, strength, intelligence, dexterity, wisdom) VALUES %s"

        rpg = RpgQueries()
        result = rpg.get_all_characters()
        
        
        cursor = self.connection.cursor()
        execute_values(cursor, insertion_query, result)         

        # execute_values(cursor, insertion_query, [
        # (5, 'superman', 12, 10, 15, 20, 25, 80, 53)
        # ])         
        self.connection.commit() 
        cursor.close()


def main():

    ins = RpgInsert()
    ins.create_table()
    ins.insert_characters()

if __name__ == "__main__":
    main()
