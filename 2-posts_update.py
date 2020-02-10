import requests as req
from bs4 import BeautifulSoup as BS
import json
import re
from itp import itp
import pandas as pd
import datetime
import numpy as np
import time
from snownlp import SnowNLP
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib
from tqdm import tqdm



base_url = 'https://instagram.com/'

def fetchData(user):
    try:
        now= datetime.datetime.now()
        t= now.strftime("%Y-%m-%d %H:%M")
        print(user)
        url= base_url + user
        headers = {"User-Agent": 'Mozilla/5.0 (Windows; U; Windows NT 5.2; en-US) AppleWebKit/534.10 (KHTML, like Gecko) Chrome/8.0.558.0 Safari/534.10'}
#        proxies={"http": 'http://Gv6RH5:Wqy4ow@103.72.153.194:8000', "https": 'https://Gv6RH5:Wqy4ow@103.72.153.194:8000'}    
        response = req.get(url, headers= headers)
    #    response.cookies.clear()
        res_html = BS(response.text, features="html.parser")
        res_html = res_html.body
        script = res_html.find('script', text=re.compile('.+sharedData.+'))
        script = script.text
        start_of_json = script.find('{')
        json_ob = script[start_of_json:-1]  # -1 means ignore the ending colon
        json_ob = json.loads(json_ob)
        posts = json_ob['entry_data']['ProfilePage'][0]['graphql']['user']['edge_owner_to_timeline_media']['edges']
        print("--------------")
    
            
        index = 1#for keeping count of post
        list_posts=[]
        for each_post in posts:
            list_csv=[]
    
            print("post number", index)
            
            temp = []
            temp.append(each_post['node']['id'])
            link=each_post['node']['thumbnail_src']
            link_encoded= urllib.parse.unquote(link)
            temp.append(link_encoded)
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
            print(post_shortcode)
    
        
            list_csv.append(user)
            list_csv.append(temp[0])
            list_csv.append(temp[1])
            list_csv.append(temp[2])
            list_csv.append(temp[4])
            list_csv.append(temp[5])
            list_csv.append(temp[6])
            store= 'www.instagram.com/p/'+ str(temp[6])
            list_csv.append(store)
            list_csv.append(t)
            list_csv.append(t)
                
            list_posts.append(list_csv)
    #         -----------comment parsing section
    
            index=index+1
    #        ind=ind+1
            if(index==6): #grabbing first 5 posts only
                break
        list_csv_all.append(list_posts)
        delays = [2,3,4,5,6]
        delay = np.random.choice(delays)
        time.sleep(delay)  
    except Exception as e:
        print(e)
        
#    return ind

          
df=pd.read_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/2-user_posts.csv')#reading database
#df=df.truncate(before=-1, after=-1)
df_list= pd.read_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/0-usernames-list.csv")#reading username list database
l=list(df_list['username'])
#i=int(len(df)/5)#for keeping count of username
#ind=len(df) #for entering data in dataframe
l1=l[8650:len(l)]

#l1=[]
#for a in l:
#    if a not in l2:
#        l1.append(a)
        
        


with ThreadPoolExecutor(max_workers=10) as executor: #max_workers=None means we consider maximum workers possible
    list_csv_all=[]
    start = time.time()
    futures = [ executor.submit(fetchData, user) for user in l1 ]
    results = []
    for result in as_completed(futures):
        results.append(result)
    end = time.time()

    print("Time Taken: {:.2f}s".format(end-start))
    




df2= pd.read_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/2-user_posts.csv")
#df2=df2.truncate(before=-1, after=-1) #deleting all rows of the dataframe
i=len(df2)
for a in list_csv_all:
    for b in a:
        
        df2.at[i,'username']=b[0]
        df2.at[i,'post_id']=b[1]
        df2.at[i,'image_url']=b[2]
        df2.at[i,'post_caption']=b[3]
        df2.at[i,'post_likes']=b[4]
        df2.at[i,'post_comments']=b[5]
        df2.at[i,'post_shortcode']=b[6]
        df2.at[i,'post_url']=b[7]
        df2.at[i,'created_date']= b[8]
        df2.at[i,'updated_date']= b[9] 
        i=i+1

df2.to_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/2-user_posts.csv", index=False)
#
###
###
###
#
#
#
##
##
df2= pd.read_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/2-user_posts.csv")

#length_df= len(df2)
for index, row in df2.iterrows():
    user= df2.at[index,'username']
    print(user)
    if(df_list.loc[df_list['username'] == user, 'id'].iloc[0]):
        df2.at[index,'account_id']= df_list.loc[df_list['username'] == user, 'id'].iloc[0]

df2.to_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/2-user_posts.csv", index=False)
#



#import urllib
#
#urllib.request.urlretrieve("https://instagram.fhkg3-2.fna.fbcdn.net/v/t51.2885-15/sh0.08/e35/c0.180.1440.1440a/s640x640/80645499_119373069283551_5621653655281456370_n.jpg?_nc_ht=instagram.fhkg3-2.fna.fbcdn.net&_nc_cat=1&_nc_ohc=R-RbRpak3CUAX9Lr-Cv&oh=593b7ed73275f1df4fd6956bbfb9703b&oe=5EA90C2F", "/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/abc.jpg")

#l.index('setriaglutathione')


#updating posts table
#adding comments_list, chinese score, english_score, avg score for each post

#updating websites table
#adding overall sentiment for each username


dic_emojis={'😘': 'Positive ',\
            '😀': 'Positive ',\
            '😍': 'Positive ',\
            '👍': 'Positive ',\
            '🤗': 'Positive ',\
            '😐': 'Neutral ',\
            '😑': 'Neutral ',\
            '😋': 'Positive ',\
            '🙂': 'Positive ',\
            '🤟': 'Positive ',\
            '😊': 'Positive ',\
            '😏': 'Negative ',\
            '😒': 'Negative ',\
            '🙄': 'Negative ',\
            '😬' : 'Negative ',\
            '👌' : 'Positive ',\
            '❤️' : 'Positive ',\
            '🔥': 'Positive ',\
            '🤔': 'Neutral ',\
           '👏' : 'Positive ' \
          
            
           } #emoji list added


    
def scrape(url):
    try:
        list_csv=[]


        print(url)
    #         url= 'http://'+ df.loc[r, "post_url"]  #post to be analysed for sentiment 
        headers = {"User-Agent": 'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/535.2 (KHTML, like Gecko) Chrome/15.0.872.0 Safari/535.2'}
        proxies={"http": 'http://rNN3b1:PLaFxy@168.235.95.186:36176', "https": 'https://rNN3b1:PLaFxy@168.235.95.186:36176'} 
        response = req.get(url, headers= headers, proxies = proxies)    
        res_html = BS(response.text, features="html.parser")
        res_html = res_html.body
        script = res_html.find('script', text=re.compile('.+sharedData.+'))
        script = script.text
        start_of_json = script.find('{')
        json_ob = script[start_of_json:-1]  # -1 means ignore the ending colon
        json_ob = json.loads(json_ob)

        #print(json_ob)
        posts = json_ob['entry_data']['PostPage'][0]['graphql']['shortcode_media']
        list1=posts['edge_media_to_parent_comment']['edges']
        list_comments=[]
        for each_post in list1:
            text_comment= each_post['node']['text']

            list_comments.append(text_comment)
    #        print(list_comments)
        timestamp = json_ob['entry_data']['PostPage'][0]['graphql']['shortcode_media']['taken_at_timestamp']
        dt_object = datetime.datetime.fromtimestamp(timestamp)
#        print(dt_object)
        total=0
        i=0
        english=0
        chinese=0
        for comment in list_comments:
    #            print('Comment:',comment)
            text=comment #put comment to analyse
            encoded_text= text.encode('utf-8')
            #print(str(encoded_text))
            for key in dic_emojis:
                encoded_emoji= key.encode('utf-8')
                #print(encoded_emoji)
                value= str.encode(dic_emojis[key])
                #print(value)
                if(encoded_emoji in encoded_text):
                    #print('true')
                    encoded_text= encoded_text.replace(encoded_emoji, b' '+ value)
            #print(encoded_text)
            text =encoded_text.decode('utf-8')
            s = SnowNLP(text)
            s.han #Traditional Chinese to Simplified Chinese

            li = re.findall(r'[\u4e00-\u9fff]+', text)#found chinese text
            if(len(li)>0):
                score_chinese= s.sentiments
            else:
                score_chinese= 0.5#does not have chinese text
    #            print('Chinese Text Opinion:',score_chinese)
            sid = SentimentIntensityAnalyzer()
            ss = sid.polarity_scores(text)

            score_eng = ( (ss['compound'] - -1) / (1 - -1) ) * (1 - 0) + 0
    #            print('English Text / Emoji Opinion:',score_eng)

            if(score_chinese>score_eng):
                each_com_score= score_chinese
            else:
                each_com_score= score_eng
            if(len(text)>30 and (score_chinese<0.2 or score_eng<0.2)):
                each_com_score=-1 #incase of advertisements we have to ignore them in performing sentiment analysis

    #            print('Respose:',each_com_score)

            if(each_com_score!=-1):
                total= total+each_com_score
                english= english+ score_eng
                chinese= chinese+ score_chinese
                i=i+1

        if(i!=0):
            avg=total/i
        else:
            avg=-1
            
        if(i!=0):
            avg_chinese= chinese/i
        else:
            avg_chinese=-1
            
        if(i!=0):
            avg_english= english/i
        else:
            avg_english=-1
            
        value_avg=round(avg,2)
        value_avg_chinese=round(avg_chinese,2)
        value_avg_english=round(avg_english,2)
#        print('Final Response:',value_avg)

        list_csv.append(url)
        list_csv.append(value_avg)
        list_csv.append(list_comments)
        list_csv.append(value_avg_english)
        list_csv.append(value_avg_chinese)
        list_csv.append(dt_object)
#        print(list_csv)
        list_csv_all.append(list_csv)
        delays = [2,3,4,5]
        delay = np.random.choice(delays)
        time.sleep(delay)
    except Exception as e:
        print(e)
    



df = pd.read_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/2-user_posts.csv')
df[['comments_list']] = df[['comments_list']].astype('object')

#all_urls=[]
#df = pd.read_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/2-user_posts.csv')
#for r in range(36000, len(df)):
#    try:
#        url= 'http://'+ df.loc[r, "post_url"]
#        all_urls.append(url)
#    except Exception as e:
#        print(e)
## print(all_urls)

import pandas as pd
import numpy as np
df = pd.read_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/2-user_posts.csv')
all_urls=[]
df[['post_datetime']] = df[['post_datetime']].fillna(value=0)
for r in range(0,len(df)):
    try:
        if(df.loc[r, "post_datetime"]==0):
            url= 'http://'+ df.loc[r, "post_url"]
            all_urls.append(url)
    except Exception as e:
        print(e)
print(len(all_urls))
        
        
with ThreadPoolExecutor(max_workers= 10) as executor: #max_workers=None means we consider maximum workers possible
    list_csv_all=[]
    start = time.time()
    futures = [ executor.submit(scrape, url) for url in all_urls ]
    results = []
    for result in as_completed(futures):
        results.append(result)
    end = time.time()

    print("Time Taken: {:.2f}s".format(end-start))
    
    
    
    
#writing the data in the list to csv file

df2= pd.read_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/2-user_posts.csv")

for a in list_csv_all:
    url_post= a[0].split('//')
#     print(a[2])
#     url_post= a[0]
    df2.loc[df2['post_url'] == url_post[1] , 'sentiment_score']= a[1]
    df2.loc[df2['post_url'] == url_post[1] , 'comments_list']= str(a[2])
    df2.loc[df2['post_url'] == url_post[1] , 'sentiment_score_english']= a[3]
    df2.loc[df2['post_url'] == url_post[1] , 'sentiment_score_chinese']= a[4]
    df2.loc[df2['post_url'] == url_post[1] , 'post_datetime']= a[5]

df2.to_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/2-user_posts.csv", index=False)

#        
# 
#
##
##
##
##       
#
df= pd.read_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/2-user_posts.csv')
df.comments_list= df.comments_list.str.strip('[]')
df.to_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/2-user_posts.csv", index=False)#writing tags to the csv file
##
##
##
df= pd.read_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/2-user_posts.csv')
df['sentiment_score'].fillna(-1, inplace=True)
df['sentiment_score_english'].fillna(-1, inplace=True)
df['sentiment_score_chinese'].fillna(-1, inplace=True)
df.to_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/2-user_posts.csv", index=False)#writing tags to the csv file
##
##
#
df= pd.read_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/2-user_posts.csv')
df_website= pd.read_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/4-details_website.csv')
df_list= pd.read_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/0-usernames-list.csv")#reading username list database

l=list(df_list['username'])
length= len(l)
#df_website['sentiment_score']=''


for index, row in df_list.iterrows():
    try:
        id_value=row['id']
        print(id_value)
        list1= list(df.loc[df['account_id'] == id_value, 'sentiment_score'].values)
        print(list1)
        list1 = [incom for incom in list1 if str(incom) != 'nan']
        print(list1)
        sum1 = sum(list1)
        print(sum1)
        avg= sum1/len(list1)
        print(avg)
        value_avg=round(avg,2)
        print(value_avg)
        df_website.loc[df_website['account_id'] == id_value, 'sentiment_overall']=value_avg
    
    
    except ZeroDivisionError:
        print('Zero Division Error')
    except:
        print('Error Occurred')



df_website.to_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/4-details_website.csv',index=False)
##
##
##
df_website= pd.read_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/4-details_website.csv')
df_website['sentiment_overall'].fillna(-1, inplace=True)
df_website.to_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/4-details_website.csv',index=False)
##
#
##
###add tags column
##
from itp import itp
import re
par = itp.Parser()
df= pd.read_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/2-user_posts.csv', error_bad_lines=False)
df[['tags_eng']] = df[['tags_eng']].astype('object')
df[['tags_eng_chinese']] = df[['tags_eng_chinese']].astype('object')

for t in tqdm(range(0,len(df))):
    try:

        post_caption= df.loc[t, "post_caption"]
        
        capt= post_caption
    #        print(capt)
        result1 = par.parse(capt)
        tags_english= result1.tags
        tags_english = list(dict.fromkeys(tags_english))
        print(tags_english)
        df.at[t,'tags_eng']= tags_english
        
        tags_chinese_english= re.findall(r"#(\w+)", capt)
        tags_chinese_english = list(dict.fromkeys(tags_chinese_english))
        df.at[t,'tags_eng_chinese']= tags_chinese_english
    except Exception as e:
        print(e)
                   
df.to_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/2-user_posts.csv',index=False)

#
#    
#
df= pd.read_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/2-user_posts.csv')
df.tags_eng= df.tags_eng.str.strip('[]')
df.tags_eng_chinese= df.tags_eng_chinese.str.strip('[]')
df.to_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/2-user_posts.csv", index=False)#writing tags to the csv file
#
df= pd.read_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/2-user_posts.csv')
df['tags_eng'].fillna('no_tags', inplace=True)
df['tags_eng_chinese'].fillna('no_tags', inplace=True)

df.to_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/2-user_posts.csv", index=False)#writing tags to the csv file
#
#
#
df= pd.read_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/2-user_posts.csv')
df[['tags_chinese']] = df[['tags_chinese']].astype('object')
for t in range(0,len(df)):
    c= df.loc[t, "tags_eng_chinese"]
    chi= re.findall(r'[\u4e00-\u9fff]+', c)
    df.at[t,'tags_chinese']= chi



df.to_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/2-user_posts.csv", index=False)#writing tags to the csv file
#
df= pd.read_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/2-user_posts.csv')
df.tags_chinese= df.tags_chinese.str.strip('[]')

df.to_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/2-user_posts.csv", index=False)#writing tags to the csv file
#
df= pd.read_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/2-user_posts.csv')
df['tags_chinese'].fillna('no_tags', inplace=True)

df.to_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/2-user_posts.csv", index=False)#writing tags to the csv file
#
#


df= pd.read_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/2-user_posts.csv')
df['comments_list'].fillna('no_comments', inplace=True)

df.to_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/2-user_posts.csv", index=False)#writing tags to the csv file






import pandas as pd
import re

df= pd.read_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/2-user_posts.csv")

def pretty(s):
    if str(s)=='nan':
        return ''
    s = s.strip().replace('\r', '')
    s = re.sub(r' +', ' ', s)
    s = s.replace('\n', '\t')
    s = s.replace('\\n', '\t')
    s = s.replace('\\r', '')
    return s

print(df['comments_list'][2])
df['post_caption'] = df['post_caption'].map(pretty)
df['comments_list'] = df['comments_list'].map(pretty)

print(df['comments_list'][2])

df.to_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/2-user_posts.csv", index=False)#writing tags to the csv file




#------------------------------------------------------------------------------


import mysql.connector
import csv
import numpy as np

#writing csv file to mysqldb

mydb = mysql.connector.connect(
  host='113.52.134.24',
  database='popared',
  user='popared',
  passwd='Iampopared2019',
  allow_local_infile=True
)
cursor = mydb.cursor()

#cursor.execute("CREATE TABLE popared.ig_post_details \
#                 (post_table_id INT AUTO_INCREMENT NOT NULL,\
#                 username VARCHAR(255), \
#                  post_id VARCHAR(255), \
#                  image_url VARCHAR(255), \
#                  post_caption nVARCHAR(3000), \
#                  post_likes INT, \
#                  post_comments INT, \
#                  post_shortcode VARCHAR(255), \
#                  post_url VARCHAR(255),\
#                  created_date DATETIME, \
#                  updated_date DATETIME, \
#                  account_id INT,\
#                  comments_list nVARCHAR(5000), \
#                  sentiment_score DECIMAL (10, 2), \
#                  sentiment_score_english DECIMAL (10, 2), \
#                  sentiment_score_chinese DECIMAL (10, 2), \
#                  tags_eng VARCHAR(2000),\
#                  tags_chinese nVARCHAR(2000),\
#                  tags_eng_chinese nVARCHAR(2000), \
#                  FOREIGN KEY (account_id) REFERENCES ig_usernames(id),\
#                  PRIMARY KEY (post_table_id))")

cursor.execute('TRUNCATE TABLE popared.ig_post_details')

cursor.execute("ALTER TABLE\
    ig_post_details\
    CONVERT TO CHARACTER SET utf8mb4\
    COLLATE utf8mb4_unicode_ci")

cursor.execute("LOAD DATA LOCAL INFILE '/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/2-user_posts.csv' \
                INTO TABLE popared.ig_post_details\
                CHARACTER SET utf8mb4 \
                FIELDS TERMINATED BY ',' \
                OPTIONALLY ENCLOSED BY '\"'\
                LINES TERMINATED BY '\n' \
                IGNORE 1 LINES (username,post_id,image_url,post_caption,post_likes,post_comments,post_shortcode,post_url,created_date,updated_date,account_id,comments_list,sentiment_score,sentiment_score_english,sentiment_score_chinese,tags_eng,tags_chinese,tags_eng_chinese, post_datetime)")


cursor.execute('ALTER TABLE ig_post_details MODIFY COLUMN updated_date date')

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
  passwd='Iampopared2019',
  allow_local_infile=True
)
cursor = mydb.cursor()

#cursor.execute("CREATE TABLE popared.ig_post_details \
#                 (post_table_id INT AUTO_INCREMENT NOT NULL,\
#                 username VARCHAR(255), \
#                  post_id VARCHAR(255), \
#                  image_url VARCHAR(255), \
#                  post_caption nVARCHAR(3000), \
#                  post_likes INT, \
#                  post_comments INT, \
#                  post_shortcode VARCHAR(255), \
#                  post_url VARCHAR(255),\
#                  created_date DATETIME, \
#                  updated_date DATETIME, \
#                  account_id INT,\
#                  comments_list nVARCHAR(5000), \
#                  sentiment_score DECIMAL (10, 2), \
#                  sentiment_score_english DECIMAL (10, 2), \
#                  sentiment_score_chinese DECIMAL (10, 2), \
#                  tags_eng VARCHAR(2000),\
#                  tags_chinese nVARCHAR(2000),\
#                  tags_eng_chinese nVARCHAR(2000), \
#                  FOREIGN KEY (account_id) REFERENCES ig_usernames(id),\
#                  PRIMARY KEY (post_table_id))")



cursor.execute("ALTER TABLE\
    ig_post_details_history\
    CONVERT TO CHARACTER SET utf8mb4\
    COLLATE utf8mb4_unicode_ci")

cursor.execute("LOAD DATA LOCAL INFILE '/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/2-user_posts.csv' \
                INTO TABLE popared.ig_post_details_history\
                CHARACTER SET utf8mb4 \
                FIELDS TERMINATED BY ',' \
                OPTIONALLY ENCLOSED BY '\"'\
                LINES TERMINATED BY '\n' \
                IGNORE 1 LINES (username,post_id,image_url,post_caption,post_likes,post_comments,post_shortcode,post_url,created_date,updated_date,account_id,comments_list,sentiment_score,sentiment_score_english,sentiment_score_chinese,tags_eng,tags_chinese,tags_eng_chinese, post_datetime)")


cursor.execute('ALTER TABLE ig_post_details_history MODIFY COLUMN updated_date date')

mydb.commit()
cursor.close()
print("Done")