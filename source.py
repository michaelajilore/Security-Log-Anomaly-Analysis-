import mysql.connector
import time
import os


lo = os.path.join(os.path.dirname(__file__), 'web-server-access-logs', 'access.log' )

thenonosquare = [None]

try:
    conn = mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        database=os.getenv("DB_NAME")
    )

    if conn.is_connected():
        print("Connection to MySQL was successful.")
        cursor = conn.cursor()
        cursor.execute("SELECT DATABASE();")
        print(" Connected to database:", cursor.fetchone()[0])
        cursor.close()

    time.sleep(4)
    
except:
    print("ya shit failed lil bro")

def cleandat():
    with open(lo, 'r') as file:
        for line in file:
            line = line.strip()
            for char in line:
                if char in thenonosquare:
                    print("needs removing")
            
    