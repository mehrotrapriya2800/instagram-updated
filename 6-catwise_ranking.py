import pandas as pd
import datetime
import random
from ast import literal_eval

df2=pd.read_csv('4-details_website.csv')
df3=pd.read_csv('5-category-detected.csv')
df_list= pd.read_csv("0-usernames-list.csv")
df4=pd.read_csv('7-grading_KOL.csv')

l=list(df_list['username'])
list_categories=['Food','Beauty','Travel','Sports','Fashion','Art_Design','Health_Fitness','Technology','News','Comedy_Humor','Pets',\
                 'Family_Parenting','Music_Dance','Home_Decor','Business','Entertainment','Lifestyle']

df=pd.DataFrame(columns=['category','rising_stars','rising_stars_id','created_date','updated_date',])

i=0
for cat in list_categories:
    list_cat1=list(df3.loc[df3['primary_category_0'] == cat, 'username'])
    list_cat2=list(df3.loc[df3['primary_category_1'] == cat, 'username'])
    list_cat3=list(df3.loc[df3['primary_category_2'] == cat, 'username'])
    
    list1= list_cat1 + list_cat2 + list_cat3 #all usernames with primary category specified
#    print(list1)
    
    list_score=[]
    for a in list1:
        if(len(list(df4.loc[df4['username'] == a, 'score']))==1):
            score= list(df4.loc[df4['username'] == a, 'score'])
            score1=score[0]
            list_score.append(score1) #list of all the scores of above usernames
            
#    print(list_score)
    list_score = list(dict.fromkeys(list_score))  
    #        
    if(len(list_score)!=0):
        max_score1= max(list_score)
        list_score.remove(max_score1)
        
#    else:
#        max_score1=0.0
        
    print(cat, max_score1)
        
    

    if(len(list_score)!=0):
        max_score2= max(list_score)
        
#    else:
#        max_score2=0.0
    print(max_score2)
        

#    print(max_score)
    rising_star=[]
    for a in list1:
        if(len(list(df4.loc[df4['username'] == a, 'score']))==1):
            score= list(df4.loc[df4['username'] == a, 'score'])
            if(score[0]==max_score1 or score[0]==max_score2):
                rising_star.append(a) #append all usernames with score equal to max score
    
    
    list3= rising_star
#    
#    list3=[]
#    if(len(rising_star)>10):
#        list3=list(random.sample(rising_star,10))
#    else:
#        list3=rising_star
    
    
    print(cat,':' ,list3)
    
    list_id=[]
    for user1 in list3:
        id1= df_list.loc[df_list['username'] == user1, 'id'].iloc[0]
        list_id.append(int(id1))
        
    now= datetime.datetime.now()
    t= now.strftime("%Y-%m-%d %H:%M")
    
    df.at[i,'category']=cat
    df.at[i,'rising_stars_id']=list_id
    df.at[i,'rising_stars']=list3
    df.at[i,'created_date']=t
    df.at[i,'updated_date']=t
    
    i=i+1
    
df.to_csv('6-catwise_risingstar.csv',index=False)
##
###
df=pd.read_csv('6-catwise_risingstar.csv')  
df.rising_stars= df.rising_stars.str.strip('[]') 
df.rising_stars_id= df.rising_stars_id.str.strip('[]')    
df.to_csv('6-catwise_risingstar.csv',index=False)   















import mysql.connector
import csv 

mydb = mysql.connector.connect(
  host='113.52.134.24',
  database='popared',
  user='popared',
  passwd='Iampopared2019'
)
cursor = mydb.cursor()


#cursor.execute("CREATE TABLE popared.ig_catwise_risingstar \
#              (table_id INT AUTO_INCREMENT NOT NULL,\
#              category VARCHAR(200),\
#              rising_stars VARCHAR(600),\
#              rising_stars_id VARCHAR(500),\
#              created_date DATETIME,\
#              updated_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\
#              PRIMARY KEY (table_id))")
cursor.execute('TRUNCATE TABLE popared.ig_catwise_risingstar')

cursor.execute("LOAD DATA LOCAL INFILE '/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/6-catwise_risingstar.csv' \
                INTO TABLE popared.ig_catwise_risingstar \
                CHARACTER SET utf8mb4 \
                FIELDS TERMINATED BY ',' \
                OPTIONALLY ENCLOSED BY '\"'\
                LINES TERMINATED BY '\n' \
                IGNORE 1 LINES (category, rising_stars, rising_stars_id, created_date,updated_date)")

cursor.execute('ALTER TABLE ig_catwise_risingstar MODIFY COLUMN updated_date date')

mydb.commit()
cursor.close()
print("Done")




import mysql.connector
import csv 

mydb = mysql.connector.connect(
  host='113.52.134.24',
  database='popared',
  user='popared',
  passwd='Iampopared2019'
)
cursor = mydb.cursor()


#cursor.execute("CREATE TABLE popared.ig_catwise_risingstar_history \
#              (table_id INT AUTO_INCREMENT NOT NULL,\
#              category VARCHAR(200),\
#              rising_stars VARCHAR(600),\
#              rising_stars_id VARCHAR(500),\
#              created_date DATETIME,\
#              updated_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\
#              PRIMARY KEY (table_id))")


cursor.execute("LOAD DATA LOCAL INFILE '/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/6-catwise_risingstar.csv' \
                INTO TABLE popared.ig_catwise_risingstar_history \
                CHARACTER SET utf8mb4 \
                FIELDS TERMINATED BY ',' \
                OPTIONALLY ENCLOSED BY '\"'\
                LINES TERMINATED BY '\n' \
                IGNORE 1 LINES (category, rising_stars, rising_stars_id, created_date,updated_date)")

cursor.execute('ALTER TABLE ig_catwise_risingstar_history MODIFY COLUMN updated_date date')

mydb.commit()
cursor.close()
print("Done")

