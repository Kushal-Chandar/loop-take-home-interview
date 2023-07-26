import psycopg2, os
from dotenv import load_dotenv

load_dotenv()

HOST=os.getenv('HOST')
DBNAME=os.getenv('DBNAME')
USER=os.getenv('USER')
PASSWORD=os.getenv('PASSWORD')
PORT=os.getenv('PORT')


conn = psycopg2.connect(host=HOST, dbname=DBNAME, user=USER, password=PASSWORD, port=PORT)

cur = conn.cursor()

# sql commands
cur.execute("""SELECT * FROM business_hours""")

print(cur.rowcount)
print(cur.rownumber)

cur.execute("""SELECT * FROM store_status""")

print(cur.rowcount)
print(cur.rownumber)

cur.execute("""SELECT * FROM store_timezones""")

print(cur.rowcount)
print(cur.rownumber)
# sql commands end

conn.commit()

cur.close()
conn.close()
