import pandas as pd
import math
import random
import datetime
from tqdm import tqdm


import pandas as pd
import math
import random
import datetime
from tqdm import tqdm

import pandas as pd

df1=pd.read_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/5-category-detected.csv', index_col='username')
df2=pd.read_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/7-grade_score.csv', index_col='username')

df1 = df1[['primary_category_0']]
df2=df2[['grade']]
df3= pd.merge(df1,df2, left_index=True, right_index=True)
df4= df3.groupby(['primary_category_0','grade'])
list_all=[]
for u in df4:
    list1=[]
    users=u[1].index.tolist()
    cat= u[0][0]
    grade= u[0][1]
    print(users)
    print(cat)
    print(grade)
    
    list1.append(users)
    list1.append(cat)
    list1.append(grade)
    list_all.append(list1)
    
dfl= pd.DataFrame(columns=['usernames','category','grade'])
i=len(dfl)
for a in range(len(list_all)):

    dfl.at[i,'usernames']= list_all[a][0]
    dfl.at[i,'category']= list_all[a][1]
    dfl.at[i,'grade']= list_all[a][2]
    
    i=i+1
    
    
    

df_list= pd.read_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/0-usernames-list.csv")
df4=pd.read_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/7-grading_KOL.csv')
df4=df4.truncate(before=-1, after=-1)
l=list(df_list['username'])
df_grades=pd.read_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/7-grade_score.csv')
df3=pd.read_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/5-category-detected.csv')

for i in tqdm(range(0,len(l))):
    try:
    
        user=l[i]
        cat= df3.loc[df3['username'] == user, 'primary_category_0'].values[0]
        grade = df_grades[df_grades['username']==user]['grade'].values[0]
        score = df_grades[df_grades['username']==user]['score'].values[0]
        print(cat, grade)
        list_recommend= dfl[(dfl.grade==grade) & (dfl.category==cat)].usernames
        list_recommend= list_recommend.values.tolist()[0]
    #     print(list_recommend)

        list1=[]
        if(len(list_recommend)>5):
            list1=list(random.sample(list_recommend,5))
        else:
            list1=list_recommend

        list_id=[]
        for user1 in list1:
            id1= df_list.loc[df_list['username'] == user1, 'id'].values[0]
            id1=int(id1)
            list_id.append(id1)

        if(len(df_list.loc[df_list['username'] == user, 'id'].values)>=1):
            df4.at[i,'account_id']= df_list.loc[df_list['username'] == user, 'id'].iloc[0]

#         print(list_id, list1)
        
        df4.at[i,'username']=user
        df4.at[i,'score']=score
        df4.at[i,'grade']=grade
        df4.at[i,'similar_kol']=list_id
        now= datetime.datetime.now()
        t= now.strftime("%Y-%m-%d %H:%M")
        df4.at[i,'created_date']= t
        df4.at[i,'updated_date']= t

        df4.to_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/7-grading_KOL.csv',index=False)

    except Exception as e:
        print(e)

    
#top_rankers= list(df4.loc[df4['grade'] == 'A+', 'username'])
#print('Top Ranked KOLs are:', top_rankers)
##    
#df4=pd.read_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/7-grading_KOL.csv')    
#df4.similar_kol= df4.similar_kol.str.strip('[]')
#df4.to_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/7-grading_KOL.csv", index=False)    
#####    
#####    
#df4=pd.read_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/7-grading_KOL.csv')    
#df4.grade= df4.grade.str.strip('[]')
#df4.grade= df4.grade.str.strip("''")
#
#df4.to_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/7-grading_KOL.csv", index=False)  
##    
##    
import mysql.connector
import csv
import numpy as np
#
##writing csv file to mysqldb
#
#mydb = mysql.connector.connect(
#  host='113.52.134.24',
#  database='popared',
#  user='popared',
#  passwd='Iampopared2019'
#)
#cursor = mydb.cursor()
#
#
#data_path = '//Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/7-grading_KOL.csv'
#with open(data_path, 'r') as f:
#    reader = csv.reader(f, delimiter=',')
#    next(reader, None)  # skip the headers
#    for row in reader:
#        
#        cursor.execute('INSERT INTO popared.grades_and_similar(account_id, username, \
#        score, grade, similar_kol, created_date) VALUES(%s,%s,%s,%s,%s,%s)', \
#                       (row[0], row[1], row[2], row[3], row[4], row[5]))
#
#
#mydb.commit()
#cursor.close()
#print("Done")    
###
###
###
###
###
##
#
mydb = mysql.connector.connect(
  host='113.52.134.24',
  database='popared',
  user='popared',
  passwd='Iampopared2019'
)
cursor = mydb.cursor()

#cursor.execute("CREATE TABLE popared.ig_grades_and_similar \
#              (tab_id INT AUTO_INCREMENT NOT NULL,\
#              account_id INT,\
#              username VARCHAR(50), \
#              score INT, \
#              grade VARCHAR(10),\
#              similar_kol VARCHAR(300),\
#              created_date DATETIME,\
#              updated_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\
#              FOREIGN KEY (account_id) REFERENCES usernames(id),\
#              PRIMARY KEY (tab_id))")

cursor.execute('TRUNCATE TABLE popared.ig_grades_and_similar')
cursor.execute("LOAD DATA LOCAL INFILE '/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/7-grading_KOL.csv' \
                INTO TABLE popared.ig_grades_and_similar \
                CHARACTER SET utf8mb4 \
                FIELDS TERMINATED BY ',' \
                OPTIONALLY ENCLOSED BY '\"'\
                LINES TERMINATED BY '\n' \
                IGNORE 1 LINES (account_id,username,score, grade,similar_kol, created_date,updated_date)")



cursor.execute('ALTER TABLE ig_grades_and_similar MODIFY COLUMN updated_date date')

mydb.commit()
cursor.close()
print("Done")





mydb = mysql.connector.connect(
  host='113.52.134.24',
  database='popared',
  user='popared',
  passwd='Iampopared2019'
)
cursor = mydb.cursor()

#cursor.execute("CREATE TABLE popared.ig_grades_and_similar_history \
#              (tab_id INT AUTO_INCREMENT NOT NULL,\
#              account_id INT,\
#              username VARCHAR(50), \
#              score INT, \
#              grade VARCHAR(10),\
#              similar_kol VARCHAR(300),\
#              created_date DATETIME,\
#              updated_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\
#              FOREIGN KEY (account_id) REFERENCES usernames(id),\
#              PRIMARY KEY (tab_id))")

cursor.execute("LOAD DATA LOCAL INFILE '/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/7-grading_KOL.csv' \
                INTO TABLE popared.ig_grades_and_similar_history \
                CHARACTER SET utf8mb4 \
                FIELDS TERMINATED BY ',' \
                OPTIONALLY ENCLOSED BY '\"'\
                LINES TERMINATED BY '\n' \
                IGNORE 1 LINES (account_id,username,score, grade,similar_kol, created_date,updated_date)")



cursor.execute('ALTER TABLE ig_grades_and_similar_history MODIFY COLUMN updated_date date')

mydb.commit()
cursor.close()
print("Done")




