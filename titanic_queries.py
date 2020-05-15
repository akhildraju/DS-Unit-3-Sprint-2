import os
import json
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.extensions import register_adapter, AsIs
from dotenv import load_dotenv


class TitanicQueries:

    DB_NAME = None
    DB_USER = None
    DB_PW = None
    DB_HOST = None

    connection = None

    def __init__(self):

        load_dotenv()
        self.DB_NAME = os.getenv("DB_NAME", default="OOPS")
        self.DB_USER = os.getenv("DB_USER", default="OOPS")
        self.DB_PW = os.getenv("DB_PW", default="OOPS")
        self.DB_HOST = os.getenv("DB_HOST", default="OOPS")
        self.connection = psycopg2.connect(dbname=self.DB_NAME, user=self.DB_USER, password=self.DB_PW, host=self.DB_HOST)
        print(self.connection)

    def get_survived_count(self):

        query = "select survived, count(*) from titanic_data group by survived order by survived"

        cursor = self.connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        print("Result=", result)
        cursor.close()
        for row in result:
            if row[0] == 0:
                print("Not Survived = ", row[1])
            else:
                print("Survived = ", row[1])

    def get_per_class(self):
        query = "select pclass, count(*) from titanic_data group by pclass order by pclass"

        cursor = self.connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        print("Result=", result)
        cursor.close()
        for row in result:
            print("class ", row[0], " = ", row[1])

    def get_per_class_survived(self):
        query = "select pclass, survived, count(*) from titanic_data group by pclass, survived order by pclass, survived"

        cursor = self.connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        print("Result=", result)
        cursor.close()
        for row in result:
            if row[1] == 0:
                print("class ", row[0], "Not Survived = ", row[2])
            else:
                print("class ", row[0], "Survived = ", row[2])

    def get_survived_avg(self):
        query = "select survived, avg(age) from titanic_data group by survived order by survived"

        cursor = self.connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        print("Result=", result)
        cursor.close()
        for row in result:
            if row[0] == 0:
                print("Average age of people who did not survive = ", row[1])
            else:
                print("Average age of people who did survive = ", row[1])

    def get_per_class_avg(self):
        query = "select pclass, avg(age) from titanic_data group by pclass order by pclass"

        cursor = self.connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        print("Result=", result)
        cursor.close()
        for row in result:
            print("Average age of class ", row[0], " = ", row[1])

    def get_fare_per_class_survival(self):
        query = "select pclass, avg(fare) from titanic_data group by pclass order by pclass"

        cursor = self.connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        # print("Result=", result)
        cursor.close()
        for row in result:
            print("Average fare for the class ", row[0], " = ", row[1])

        query = "select survived, avg(fare) from titanic_data group by survived order by survived"
        
        cursor = self.connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        for row in result:
            if row[0] == 0:
                print("Average fare for people who did not survive = ", row[1])
            else:
                print("Average fare for people who survived = ", row[1])
           
    def get_siblingspouses_avg_class_survival(self):
        query = "select pclass, avg(siblingsspouses) from titanic_data group by pclass order by pclass"

        cursor = self.connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        # print("Result=", result)
        cursor.close()
        for row in result:
            print("Average Siblibgs/Spouses for the class ", row[0], " = ", row[1])

        query = "select survived, avg(siblingsspouses) from titanic_data group by survived order by survived"
        
        cursor = self.connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        for row in result:
            if row[0] == 0:
                print("Average number of Sibling/Spouses for people who did not survive = ", row[1])
            else:
                print("Average number of Sibling/Spouses for people who survived = ", row[1])

    def get_parentschildren_avg_class_survival(self):
        query = "select pclass, avg(parentschildren) from titanic_data group by pclass order by pclass"

        cursor = self.connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        # print("Result=", result)
        cursor.close()
        for row in result:
            print("Average number of Parents/Children for the class ", row[0], " = ", row[1])

        query = "select survived, avg(parentschildren) from titanic_data group by survived order by survived"
        
        cursor = self.connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        for row in result:
            if row[0] == 0:
                print("Average number of Parents/Children for people who did not survive = ", row[1])
            else:
                print("Average number of Parents/Children for people who survived = ", row[1])

    def get_same_name_count(self):
        query = "select count(*) from titanic_data"

        cursor = self.connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        total_names = result[0][0]
        cursor.close()

        query = "select count(distinct(name)) from titanic_data"
        
        cursor = self.connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        distinct_names = result[0][0]
        cursor.close()

        if (distinct_names == total_names):
            print("There are no passengers with the same name")
        else:
            print("The are ", total_names - distinct_names, " passengers with same names")


def main():
    titanic = TitanicQueries()
    print("-----------------------------------------------------------------------------")
    titanic.get_survived_count()
    print("-----------------------------------------------------------------------------")
    titanic.get_per_class()
    print("-----------------------------------------------------------------------------")
    titanic.get_per_class_survived()
    print("-----------------------------------------------------------------------------")
    titanic.get_survived_avg()
    print("-----------------------------------------------------------------------------")
    titanic.get_per_class_avg()
    print("-----------------------------------------------------------------------------")
    titanic.get_fare_per_class_survival()
    print("-----------------------------------------------------------------------------")
    titanic.get_siblingspouses_avg_class_survival()
    print("-----------------------------------------------------------------------------")
    titanic.get_parentschildren_avg_class_survival()
    print("-----------------------------------------------------------------------------")
    titanic.get_same_name_count()
    print("-----------------------------------------------------------------------------")

if __name__ == "__main__":
    main()
