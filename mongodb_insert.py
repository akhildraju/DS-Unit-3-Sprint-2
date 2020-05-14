import os
import json
from dotenv import load_dotenv
from rpg_queries import RpgQueries
import pymongo
import pandas as pd


class MongoDBInsert:
    MDB_USER = None
    MDB_PW = None
    MDB_CLUSTER = None

    client = None

    def __init__(self):
        load_dotenv()
        self.MDB_USER = os.getenv("MDB_USER", default="OOPS")
        self.MDB_PW = os.getenv("MDB_PW", default="OOPS")
        self.MDB_CLUSTER = os.getenv("MDB_CLUSTER", default="OOPS")
        connection_uri = f"mongodb+srv://{self.MDB_USER}:{self.MDB_PW}@{self.MDB_CLUSTER}.mongodb.net/test?retryWrites=true&w=majority"
        print("URI:", connection_uri)
        self.client = pymongo.MongoClient(connection_uri)
        print("----------------")
        print("CLIENT:", type(self.client), self.client)
        print("DB NAMES:", self.client.list_database_names())

    def insert_characters(self):
        rpg = RpgQueries()
        result = rpg.get_all_characters()
        col_list = []

        for row in result:
            col = {}
            col['character_id'] = row[0]
            col['name'] = row[1]
            col['level'] = row[2]
            col['exp'] = row[3]
            col['hp'] = row[4]
            col['strength'] = row[5]
            col['intelligence'] = row[6]
            col['dexterity'] = row[7]
            col['wisdom'] = row[8]

            col_list.append(col)

        db = self.client.ds14_db
        print("DB:", type(db), db)
        collection = db.ds14_character_collection
        print("COLLECTION:", type(collection), collection)
        collection.insert_many(col_list)
        print("DOCS:", collection.count_documents({}))

    def verify_characters(self):
        db = self.client.ds14_db
        print("DB:", type(db), db)
        collection = db.ds14_character_collection
        print("DOCS:", collection.count_documents({}))
        ch = list(collection.find({"name": "Debit"}))
        print(ch)

    def insert_titanic_data(self):
        db = self.client.ds14_db
        collection = db.ds14_titanic_collection
        df = pd.read_csv("https://raw.githubusercontent.com/LambdaSchool/DS-Unit-3-Sprint-2-SQL-and-Databases/master/module2-sql-for-analysis/titanic.csv")
        print(df.head())
        print(df.dtypes)
        data = df.to_dict(orient='records')
        collection.insert_many(data)
        print("DOCS:", collection.count_documents({}))

    def verify_titanic_data(self):
        db = self.client.ds14_db
        print("DB:", type(db), db)
        collection = db.ds14_titanic_collection
        print("DOCS:", collection.count_documents({}))
        name = list(collection.find({"Name": "Mr. Charles Joseph Shorney"}))
        print(name)


def main():
    ins = MongoDBInsert()
    # ins.insert_characters()
    # ins.verify_characters()

    ins.insert_titanic_data()
    ins.verify_titanic_data()

if __name__ == "__main__":
    main()
