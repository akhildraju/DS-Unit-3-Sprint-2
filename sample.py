import os
import sqlite3

# construct a path to wherever your database exists
# DB_FILEPATH = "./chinook.db"
DB_FILEPATH = os.path.join(os.path.dirname(__file__), "rpg_db.sqlite3")
print(DB_FILEPATH)

connection = sqlite3.connect(DB_FILEPATH)
print("CONNECTION:", connection)

cursor = connection.cursor()
print("CURSOR", cursor)

query = "SELECT count(*) as count FROM charactercreator_character"

# #result = cursor.execute(query)
# #print("RESULT", result) #> returns cursor object w/o results (need to fetch the results)

result2 = cursor.execute(query).fetchall()
print("Total number of characters are:", result2[0][0])