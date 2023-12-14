import socket
import sqlite3

#creates list of SQL commands for each transaction

def create_user(user_id, user_name, timestamp):
    #Finds if user_id exists
    a0= f"SELECT user_id FROM Users WHERE user_id = '{user_id}'"
    a1= f"INSERT INTO Users VALUES ('{user_id}', '{user_name}', '{timestamp}')"
    a2= f"INSERT INTO Posts VALUES ('{user_id}', '{user_id}0','{timestamp}', '{user_name} is now on DataBook!',0)"

    nodes=[user_id,user_id,user_id]
    return [a0,a1,a2],nodes

def create_friendship(user_id1, user_id2, timestamp):
    a0=f"SELECT user_id1 FROM Friendship WHERE user_id1 = '{user_id1}' AND user_id2 = '{user_id2}'"
    a1= f"INSERT INTO Friendship VALUES ('{user_id1}', '{user_id2}', '{timestamp}')"
    a2= f"INSERT INTO Friendship VALUES ('{user_id2}', '{user_id1}', '{timestamp}')"

    nodes=[user_id1,user_id1,user_id2]
    return [a0,a1,a2],nodes

def create_post(post_id,user_id, timestamp, content):
    a0= f"SELECT post_id FROM Posts WHERE post_id = {post_id}"
    a1= f"INSERT INTO Posts VALUES ('{post_id}', '{user_id}', '{timestamp}', '{content}', 0)"
    a2= f"INSERT INTO Likes VALUES ('{user_id}',   '{post_id}', '{timestamp}')"

    nodes=[user_id,user_id,user_id]
    return [a0,a1,a2],nodes

def like_post(user_id, user_id_of_post,post_id, timestamp):
    a0=f"SELECT post_id FROM Posts WHERE post_id = '{post_id}'"
    a1= f"INSERT INTO Likes VALUES ('{user_id}', '{post_id}', '{timestamp}')"
    a2= f"UPDATE Posts SET num_likes = num_likes + 1 WHERE post_id = {post_id}"

    nodes=[user_id_of_post,user_id,user_id_of_post]
    return [a0,a1,a2],nodes

def edit_post(user_id, post_id, content):
    a0=f"SELECT post_id FROM Posts WHERE post_id = {post_id}"
    a1= f"UPDATE Posts SET content = '{content}' WHERE post_id = {post_id}"

    nodes=[user_id,user_id]
    return [a0,a1],nodes

def timeline_query(user_id):
    #Find all friends
    a0= f"SELECT user_id1 FROM Friendship WHERE user_id2 = '{user_id}'"
    a1= f"SELECT post_id, user_id, timestamp, content, num_likes FROM Posts WHERE user_id = '{user_id}'"
    return [a0,a1]

def print_all_tables():
    a0= f"SELECT * FROM Users"
    a1= f"SELECT * FROM Friendship"
    a2= f"SELECT * FROM Posts"
    a3= f"SELECT * FROM Likes"
    return [a0,a1,a2,a3]