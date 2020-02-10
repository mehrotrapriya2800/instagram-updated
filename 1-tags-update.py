import requests as req
from bs4 import BeautifulSoup as BS
import json
import re
from itp import itp
import pandas as pd
from collections import Counter
import datetime
import mysql.connector
import csv
import numpy as np
import time
from multiprocessing import Pool
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote
import os
import random
import sys

def fetchData(user, index):
    try:
        list_csv=[]
        now= datetime.datetime.now()
        t= now.strftime("%Y-%m-%d %H:%M")
        list_tags=[]
        tags = []
        url= 'https://instagram.com/' + user
        headers = {"User-Agent": 'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/532.0 (KHTML, like Gecko) Chrome/4.0.201.1 Safari/532.0'}
        no=index
        proxy = list_ip[no][0]
        proxy_port = list_ip[no][1]
        username = quote(list_ip[no][2])
        password = quote(list_ip[no][3])
        proxy_http = 'http://' + username + ':' + password + '@' + proxy + ':' + proxy_port
        proxy_https = 'https://' + username + ':' + password + '@' + proxy + ':' + proxy_port
        print(proxy_http)
        print(proxy_https)

        proxies={'http' : proxy_http, 'https': proxy_https}

        
        
        response = req.get(url, headers= headers, proxies = proxies)
        response.cookies.clear()
        res_html = BS(response.text, features="html.parser")
        res_html = res_html.body
        script = res_html.find('script', text=re.compile('.+sharedData.+'))
        script = script.text
        start_of_json = script.find('{')
        json_ob = script[start_of_json:-1]  # -1 means ignore the ending colon
        json_ob = json.loads(json_ob)
        posts = json_ob['entry_data']['ProfilePage'][0]['graphql']['user']['edge_owner_to_timeline_media']['edges']
    
        index = 1#for keeping count of post
        for each_post in posts:
            temp = []
            temp.append(each_post['node']['id'])
            temp.append(each_post['node']['thumbnail_src'])
            if (each_post['node']['edge_media_to_caption']['edges']):
                temp.append(each_post['node']['edge_media_to_caption']['edges'][0]['node']['text'])
            else:
                temp.append('no_caption')
            ts = int(each_post['node']['taken_at_timestamp'])
            temp.append(datetime.datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S'))
            temp.append(each_post['node']['edge_liked_by']['count'])
            temp.append(each_post['node']['edge_media_to_comment']['count'])
            post_shortcode = each_post['node']['shortcode']
            temp.append(post_shortcode)
            list2=temp
            capt= list2[2]
            par = itp.Parser()
            result1 = par.parse(capt)
            list_tags.append(result1.tags)
    
            for sublist in list_tags:
                for item in sublist:
                    tags.append(item)
            tag_list=[]
            most_common= Counter(tags).most_common(10)
            for a in most_common:
                tag_list.append(a[0])
            #print("Top 10 hashtags used by", user," from their 10 most recent posts",tag_list)
    
    
            index=index+1
            if(index==6): #grabbing first 5 posts only
                break
    
    
        list_csv.append(user)
        list_csv.append(tag_list)
        list_csv.append(t)
        list_csv.append(t)
        print(list_csv)
        list_csv_all.append(list_csv)
        delays = [3,4,5,6,7,8]
        delay = np.random.choice(delays)
        time.sleep(delay)      
    except Exception as e:
        print(e)
        if('ProfilePage'):
            print(index)
            index= index+1
            
            if(index!=length_ip):
                print('Next IP being used............')
                fetchData(user, index)

            else:
                print('All IPs used...............')
                sys.exit('All IPs over')
        
            
            



df_list= pd.read_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/0-usernames-list.csv")#reading username list database
l=list(df_list['username'])
df_tags= pd.read_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/1-user_tags.csv")#reading username list database
l2=list(df_tags['username'])
l1=l[50:500]
    
        
index = 0

par = itp.Parser()
list_ip=[]

with open('/Users/PopaRED2/Desktop/ip_list.txt', 'r') as fi:
    a = [_ for _ in fi.read().splitlines() if _ is not '']
    ind = 0
    for _ in a:
        ut = _.split(' ')[0].split(':')
#            print(ut)
        list_ip.append(ut)
print(list_ip)
length_ip = len(list_ip)
#no= random.randrange(length_ip)


with ThreadPoolExecutor(max_workers=10) as executor: #max_workers=None means we consider maximum workers possible
    list_csv_all=[]
    start = time.time()
     
    futures = [ executor.submit(fetchData, user, index) for user in l1 ]
    results = []
    for result in as_completed(futures):
        results.append(result)
    end = time.time()
    

    print("Time Taken: {:.2f}s".format(end-start))
    



   
df2= pd.read_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/1-user_tags.csv")
#df2=df2.truncate(before=-1, after=-1) #deleting all rows of the dataframe
i=len(df2)
for a in range(len(list_csv_all)):

    df2.at[i,'username']= list_csv_all[a][0]
    df2.at[i,'tags']= list_csv_all[a][1]
    df2.at[i,'created_date']= list_csv_all[a][2]
    df2.at[i,'updated_date']= list_csv_all[a][3]
    i=i+1

df2.to_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/1-user_tags.csv", index=False)





df2= pd.read_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/1-user_tags.csv")

for index, row in df2.iterrows():
    user= df2.at[index,'username']
    print(user)
    if(df_list.loc[df_list['username'] == user, 'id'].iloc[0]):
        df2.at[index,'account_id']= df_list.loc[df_list['username'] == user, 'id'].iloc[0]


df2.tags= df2.tags.str.strip('[]')
df2.tags.replace('nan', np.nan, inplace=True)
df2.fillna('no_tags', inplace=True)


df2.to_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/1-user_tags.csv", index=False)







def update_ig_tags():
    mydb = mysql.connector.connect(
      host='113.52.134.24',
      database='popared',
      user='popared',
      passwd='Iampopared2019'
    )
    cursor = mydb.cursor()
    
#    cursor.execute("CREATE TABLE popared.tags \
#                     (tag_id INT AUTO_INCREMENT NOT NULL,\
#                      account_id INT,\
#                      username VARCHAR(255), \
#                      tags VARCHAR(255), \
#                      created_date DATETIME, \
#                      updated_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, \
#                      FOREIGN KEY (account_id) REFERENCES usernames(id),\
#                      PRIMARY KEY (tag_id))")
    
    cursor.execute('TRUNCATE TABLE popared.ig_tags')
#    cursor.execute('ALTER TABLE popared.tags MODIFY tags VARCHAR(500)')
    cursor.execute("LOAD DATA LOCAL INFILE '/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/1-user_tags.csv' \
                    INTO TABLE popared.ig_tags \
                    CHARACTER SET utf8mb4 \
                    FIELDS TERMINATED BY ',' \
                    OPTIONALLY ENCLOSED BY '\"'\
                    LINES TERMINATED BY '\n' \
                    IGNORE 1 LINES (username,tags,created_date,updated_date,account_id)")
    
    cursor.execute('ALTER TABLE ig_tags MODIFY COLUMN updated_date date')
    
    
    
    mydb.commit()
    cursor.close()
    print("Done")

def update_ig_tags_history():
    mydb = mysql.connector.connect(
      host='113.52.134.24',
      database='popared',
      user='popared',
      passwd='Iampopared2019'
    )
    cursor = mydb.cursor()
    
#    cursor.execute("CREATE TABLE popared.tags \
#                     (tag_id INT AUTO_INCREMENT NOT NULL,\
#                      account_id INT,\
#                      username VARCHAR(255), \
#                      tags VARCHAR(255), \
#                      created_date DATETIME, \
#                      updated_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, \
#                      FOREIGN KEY (account_id) REFERENCES usernames(id),\
#                      PRIMARY KEY (tag_id))")
    
    
#    cursor.execute('ALTER TABLE popared.tags_history MODIFY tags VARCHAR(500)')
    cursor.execute("LOAD DATA LOCAL INFILE '/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/1-user_tags.csv' \
                    INTO TABLE popared.ig_tags_history \
                    CHARACTER SET utf8mb4 \
                    FIELDS TERMINATED BY ',' \
                    OPTIONALLY ENCLOSED BY '\"'\
                    LINES TERMINATED BY '\n' \
                    IGNORE 1 LINES (username,tags,created_date,updated_date,account_id)")
    
    cursor.execute('ALTER TABLE ig_tags_history MODIFY COLUMN updated_date date')
    
    
    
    mydb.commit()
    cursor.close()
    print("Done")

#
#update_ig_tags()
#update_ig_tags_history()
