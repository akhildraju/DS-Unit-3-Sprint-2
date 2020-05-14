import os
import sqlite3


class RpgQueries:

    DB_FILEPATH = os.path.join(os.path.dirname(__file__), "rpg_db.sqlite3")
    connection = None
    cursor = None

    def __init__(self):
        self.connection = sqlite3.connect(self.DB_FILEPATH)
        self.cursor = self.connection.cursor()

    def get_character_count(self):
        query = "SELECT count(*) FROM charactercreator_character"
        result = self.cursor.execute(query).fetchall()
        return result[0][0]

    def get_item_count(self):
        query = "SELECT COUNT(*) as count FROM  armory_item"
        result = self.cursor.execute(query).fetchall()
        return result[0][0]

    def get_weapon_count(self):
        query = "SELECT COUNT(*) as count FROM  armory_weapon"
        result = self.cursor.execute(query).fetchall()
        return result[0][0]

    def get_character_item_count(self):
        query = """
            SELECT a.name, count(b.item_id)
            from charactercreator_character_inventory b, charactercreator_character a
            where b.character_id = a.character_id
            group by b.character_id limit(20)"""

        result = self.cursor.execute(query).fetchall()
        return result

    def get_character_weapon_count(self):
        query = """
            SELECT
                a.character_id,
                count(a.item_id)
            FROM
                charactercreator_character_inventory a,
                armory_weapon b
            WHERE
                a.item_id = b.item_ptr_id
            GROUP BY
                a.character_id
            LIMIT (20) """

        result = self.cursor.execute(query).fetchall()
        return result

    def get_character_item_average(self):
        query = "select 1.0*count(item_id)/count(distinct character_id) from charactercreator_character_inventory"
        result = self.cursor.execute(query).fetchall()
        return result[0][0]

    def get_character_weapon_average(self):
        query = "select 1.0*count(item_id)/count(distinct character_id) from charactercreator_character_inventory where charactercreator_character_inventory.item_id in (select item_ptr_id from armory_weapon)"
        result = self.cursor.execute(query).fetchall()
        return result[0][0]

    def get_all_characters(self):
        query = "select * from charactercreator_character"
        result = self.cursor.execute(query).fetchall()
        return result


def main():
    queries = RpgQueries()
    print("Total number of characters: ", queries.get_character_count())

    item_count = queries.get_item_count()
    print("Total number of Items are: ", item_count)

    weapons_count = queries.get_weapon_count()
    print("Total number of Weapons: ", weapons_count)

    other_count = item_count - weapons_count
    print("Total number of items that are Not Weapons: ", other_count)

    result = queries.get_character_item_count()
    print("Character Item Count:")
    formatted_row = '{:30} {:10}'
    print(formatted_row.format("Character Name", "Item Count"))
    print("------------------------------------------")
    for row in result:
        print(formatted_row.format(*row))

    print("Character Weapon Count")
    result = queries.get_character_weapon_count()
    formatted_row = '{:13} {:12}'
    print(formatted_row.format("Character Id", "Weapon Count"))
    print("--------------------------")
    for row in result:
        print(formatted_row.format(*row))

    print("Character Item Average: ", queries.get_character_item_average())

    print("Character Weapon Average: ", queries.get_character_weapon_average())


if __name__ == "__main__":
    main()
