import pandas as pd
import os
import sqlite3


class BuddyMoveHoliday:

    URL = 'https://archive.ics.uci.edu/ml/machine-learning-databases/00476/buddymove_holidayiq.csv'
    DB_FILEPATH = os.path.join(os.path.dirname(__file__), "buddymove_holidayiq.sqlite3")

    connection = None
    cursor = None

    def __init__(self):
        df = pd.read_csv(self.URL)
        self.connection = sqlite3.connect(self.DB_FILEPATH)
        df.to_sql('review', self.connection, if_exists='replace', index=False)
        self.cursor = self.connection.cursor()

    def get_row_count(self):
        query = "SELECT count(*) FROM review"
        result = self.cursor.execute(query).fetchall()
        return result[0][0]

    def get_user_count(self):
        query = "SELECT count(*) FROM review where Nature >= 100 and Shopping >= 100"
        result = self.cursor.execute(query).fetchall()
        return result[0][0]

    def get_average_reviews(self):
        query = """
            SELECT
                avg(Sports),
                avg(Religious),
                avg(Nature),
                avg(Theatre),
                avg(Shopping),
                avg(Picnic)
            FROM
                review"""
        result = self.cursor.execute(query).fetchall()
        return result


def main():
    bmh = BuddyMoveHoliday()
    print("Total number of Rows: ", bmh.get_row_count())
    print("User Count with more than 100 Reviews: ", bmh.get_user_count())
    
    result = bmh.get_average_reviews()
    row = result[0]
    print("Average Sports Reviews: ", row[0])
    print("Average Religious Reviews: ", row[1])
    print("Average Nature Reviews: ", row[2])
    print("Average Theatre Reviews: ", row[3])
    print("Average Shopping Reviews: ", row[4])
    print("Average Picnic Reviews: ", row[5])


if __name__ == "__main__":
    main()
