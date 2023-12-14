import socket
import sqlite3

NUM_NODE=3
def init_db(name):

    database_name = f"{name}.db"
    conn = sqlite3.connect(database_name)
    cursor = conn.cursor()
    # Define the SQL commands to create tables
    # Example: Create a table named 'example_table'

    cursor.execute('DROP TABLE Users')
    cursor.execute('DROP TABLE Friendship')
    cursor.execute('DROP TABLE Posts')
    cursor.execute('DROP TABLE Likes')
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS Users (
                      user_id TEXT PRIMARY KEY, user_name TEXT, timestamp TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS Friendship (
                   user_id1 TEXT, user_id2 TEXT, timestamp TEXT, PRIMARY KEY (user_id1, user_id2))''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS Posts (
                   post_id INTEGER PRIMARY KEY, user_id TEXT, timestamp TEXT, content TEXT, num_likes INTEGER)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS Likes (
                     user_id TEXT, post_id INTEGER, timestamp TEXT, PRIMARY KEY (user_id, post_id))''')
    # Add more table creation commands as needed
    # ...
    
    conn.commit()

    #conn.rollback()


if __name__ == "__main__":

    for i in range(NUM_NODE):
        init_db(f"node_{i}")

    print("Database initialized.")
