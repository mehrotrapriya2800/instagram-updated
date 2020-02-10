import mysql.connector
import csv
import numpy as np

mydb = mysql.connector.connect(
 host='113.52.134.24',
 database='popared',
 user='popared',
 passwd='Iampopared2019'
)
cursor = mydb.cursor()



cursor.execute('SET FOREIGN_KEY_CHECKS = 0')
cursor.execute('TRUNCATE table popared.ig_usernames')

data_path = '//Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/0-usernames-list.csv'
with open(data_path, 'r') as f:
    reader = csv.reader(f, delimiter=',')
    next(reader, None)  # skip the headers
    for row in reader:
#         print(row[1])
        cursor.execute('INSERT INTO popared.ig_usernames(id, username) VALUES(%s,%s)', (row[0], row[1]))

cursor.execute('SET FOREIGN_KEY_CHECKS = 1')

mydb.commit()
cursor.close()
print("Done")





import mysql.connector
import csv
import numpy as np

#writing csv file to mysqldb

mydb = mysql.connector.connect(
  host='113.52.134.24',
  database='popared',
  user='popared',
  passwd='Iampopared2019'
)
cursor = mydb.cursor()
cursor.execute("CREATE TABLE popared.usernames (id INT NOT NULL,\
                username VARCHAR(255), \
                PRIMARY KEY(id))")
mydb.commit()
cursor.close()
print("Done")





import mysql.connector
import csv
import numpy as np

#writing csv file to mysqldb

mydb = mysql.connector.connect(
  host='113.52.134.24',
  database='popared',
  user='popared',
  passwd='Iampopared2019'
)
cursor = mydb.cursor()

data_path = '//Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/usernames-list.csv'
with open(data_path, 'r') as f:
    reader = csv.reader(f, delimiter=',')
    next(reader, None)  # skip the headers
    l = []
    for row in reader:
        print(row[1])
        l.append
        
cursor.execute('INSERT INTO popared.usernames(id, username) VALUES(%s,%s)', (l)

mydb.commit()
cursor.close()
print("Done")