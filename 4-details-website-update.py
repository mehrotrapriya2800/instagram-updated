#create cols : username, followers_count, interactions_per_post, engagement_rate, average_likes, average_comments, date
#interactions_per_post= likes+comments/total posts
#engagement_rate=interactions_per_post/followers_count


import requests as req
from bs4 import BeautifulSoup as BS
import json
import re
import pandas as pd
from statistics import mean
import datetime
import mysql.connector
import csv
import time
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed

second_dict = {}
base_url = 'https://instagram.com/'
payload = {
    'output': 'json',
    'apikey': '9188d7d3279adba587f1bfe0737f65ac',
    'model': ['Age', 'Gender']
}
api_url = 'https://api.haystack.ai/api/image/analyze'




def fetchData(user):
    try:
        list_csv=[]
        print(user)
        url=base_url + user
        headers = {"User-Agent": 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/525.19 (KHTML, like Gecko) Chrome/1.0.154.53 Safari/525.19'}
#        proxies={"http": 'http://SrEwYQ:3K1bys@45.32.56.105:48195', "https": 'https://SrEwYQ:3K1bys@45.32.56.105:48195'}
    
        response = req.get(url, headers= headers) 
        res_html = BS(response.text, features="html.parser")
        res_html = res_html.body
        script = res_html.find('script', text=re.compile('.+sharedData.+'))
        script = script.text
        start_of_json = script.find('{')
        json_ob = script[start_of_json:-1]  # -1 means ignore the ending colon
        json_ob = json.loads(json_ob)
    #    print(json_ob)
        posts = json_ob['entry_data']['ProfilePage'][0]['graphql']['user']['edge_owner_to_timeline_media']['edges']
        
        followers= json_ob['entry_data']['ProfilePage'][0]['graphql']['user']['edge_followed_by']['count']
    

        
        temp1 = []
        temp2=[]
        for each_post in posts:
            temp1.append(each_post['node']['edge_liked_by']['count'])
            temp2.append(each_post['node']['edge_media_to_comment']['count'])
    
        if(len(temp1)==0):
            avg_likes=0
        else:
            avg_likes= mean(temp1)
            
        if(len(temp2)==0):
            avg_comments=0
        else:
            avg_comments= mean(temp2)    
            
    
        x=((avg_likes*12) + (avg_comments*12))/12
        now= datetime.datetime.now()
        t= now.strftime("%Y-%m-%d %H:%M")
        likes=round(avg_likes,2)
        com=round(avg_comments,2)
        ipp=round(x,2)
        e_r=x/followers
        er=round(e_r,4) 
        
        list_csv.append(user)
        list_csv.append(followers)
        list_csv.append(ipp)
        list_csv.append(er)
        list_csv.append(likes)
        list_csv.append(com)
        list_csv.append(t)
        list_csv.append(t)
#        print(list_csv)
        list_csv_all.append(list_csv)
        delays = [27, 18, 21,30,35,37,29]
        delay = np.random.choice(delays)
        time.sleep(delay)
    except Exception as e:
        print(e)

#    maisyma1999


#---------------Details UPDATE


          
df_list= pd.read_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/0-usernames-list.csv")#reading username list database
l=list(df_list['username'])
df_website= pd.read_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/4-details_website.csv")#reading username list database
l2=list(df_website['username'])
#l1=l[8500:len(l)]


l1=[]
for a in l:
    if a not in l2:
        l1.append(a)
        
        


with ThreadPoolExecutor(max_workers=None) as executor: #max_workers=None means we consider maximum workers possible
    list_csv_all=[]
    start = time.time()
    futures = [ executor.submit(fetchData, user) for user in l1 ]
    results = []
    for result in as_completed(futures):
        results.append(result)
    end = time.time()

    print("Time Taken: {:.2f}s".format(end-start))
    
    
    
    
    
    


#df2=df2.truncate(before=-1, after=-1) #deleting all rows of the dataframe
df2[['created_date']] = df2[['created_date']].astype('object')
df2[['updated_date']] = df2[['updated_date']].astype('object')
i=len(df2)
for a in range(len(list_csv_all)):

    df2.at[i,'username']= list_csv_all[a][0]
    df2.at[i,'follower_count']= list_csv_all[a][1]
    df2.at[i,'interactions_per_post']= list_csv_all[a][2]
    df2.at[i,'engagement_rate']= list_csv_all[a][3]
    df2.at[i,'average_likes']= list_csv_all[a][4]
    df2.at[i,'average_comments']= list_csv_all[a][5]
    df2.at[i,'created_date']= list_csv_all[a][6]
    df2.at[i,'updated_date']= list_csv_all[a][7]
    i=i+1

df2.to_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/4-details_website.csv", index=False)
 





df_list= pd.read_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/0-usernames-list.csv")#reading username list database

df2= pd.read_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/4-details_website.csv")

for index, row in df2.iterrows():
    user= df2.at[index,'username']
    print(user)
    if(df_list.loc[df_list['username'] == user, 'id'].iloc[0]):
        df2.at[index,'account_id']= df_list.loc[df_list['username'] == user, 'id'].iloc[0]

df2.to_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/4-details_website.csv", index=False)
#





#writing csv file to mysqldb
    

mydb = mysql.connector.connect(
  host='113.52.134.24',
  database='popared',
  user='popared',
  passwd='Iampopared2019'
)
cursor = mydb.cursor()

#cursor.execute("CREATE TABLE popared.ig_website_formulae \
#              (web_id INT AUTO_INCREMENT NOT NULL,\
#              account_id INT,\
#              username VARCHAR(255), \
#              follower_count INT, \
#              interactions_per_post DECIMAL (10, 2), \
#              engagement_rate DECIMAL (10, 2), \
#              average_likes DECIMAL (10, 2), \
#              average_comments DECIMAL (10, 2),\
#              created_date DATETIME,\
#              updated_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\
#              FOREIGN KEY (account_id) REFERENCES usernames(id),\
#              PRIMARY KEY (web_id))")
# 

cursor.execute('TRUNCATE TABLE popared.ig_website_formulae')
cursor.execute("LOAD DATA LOCAL INFILE '/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/4-details_website.csv' \
                INTO TABLE popared.ig_website_formulae \
                CHARACTER SET utf8mb4 \
                FIELDS TERMINATED BY ',' \
                OPTIONALLY ENCLOSED BY '\"'\
                LINES TERMINATED BY '\n' \
                IGNORE 1 LINES (username,follower_count,interactions_per_post,engagement_rate,average_likes,average_comments,created_date,updated_date,account_id,sentiment_overall)")

cursor.execute('ALTER TABLE ig_website_formulae MODIFY COLUMN updated_date date')
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

#cursor.execute("CREATE TABLE popared.ig_website_formulae_history \
#              (web_id INT AUTO_INCREMENT NOT NULL,\
#              account_id INT,\
#              username VARCHAR(255), \
#              follower_count INT, \
#              interactions_per_post DECIMAL (10, 2), \
#              engagement_rate DECIMAL (10, 2), \
#              average_likes DECIMAL (10, 2), \
#              average_comments DECIMAL (10, 2),\
#              created_date DATETIME,\
#              updated_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\
#              FOREIGN KEY (account_id) REFERENCES usernames(id),\
#              PRIMARY KEY (web_id))")
# 


cursor.execute("LOAD DATA LOCAL INFILE '/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/4-details_website.csv' \
                INTO TABLE popared.ig_website_formulae_history \
                CHARACTER SET utf8mb4 \
                FIELDS TERMINATED BY ',' \
                OPTIONALLY ENCLOSED BY '\"'\
                LINES TERMINATED BY '\n' \
                IGNORE 1 LINES (username,follower_count,interactions_per_post,engagement_rate,average_likes,average_comments,created_date,updated_date,account_id,sentiment_overall)")

cursor.execute('ALTER TABLE ig_website_formulae_history MODIFY COLUMN updated_date date')
mydb.commit()
cursor.close()
print("Done")