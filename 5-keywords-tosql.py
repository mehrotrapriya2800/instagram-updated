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


cursor.execute("CREATE TABLE popared.keywords \
              (keyword_id INT AUTO_INCREMENT NOT NULL,\
               category VARCHAR(255), \
               keyword VARCHAR(255),\
               PRIMARY KEY (keyword_id))")
 

mydb.commit()
cursor.close()
print("Done")

#-----------------------------------------------------------------------------------


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

data_path = '//Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/5-schema-category-decider.csv'
with open(data_path, 'r') as f:
    reader = csv.reader(f, delimiter=',')
    next(reader, None)  # skip the headers
    for row in reader:
        cursor.execute('INSERT INTO popared.ig_keywords(category, keyword)\
                      VALUES(%s,%s)', (row[0], row[1]))

mydb.commit()
cursor.close()
print("Done")

#-----------------------------------------------------------------------------------