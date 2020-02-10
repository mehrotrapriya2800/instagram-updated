import requests as req
from bs4 import BeautifulSoup as BS
import json
import re
from itp import itp
import pandas as pd
import numpy as np
import datetime
import urllib.request
import cv2
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

par = itp.Parser()


def fetchData(user):
    try:
        list_csv=[]
        print(user)
        now = datetime.datetime.now()
        t= now.strftime("%Y-%m-%d %H:%M")
        url=base_url + user
        headers = {'user-agent': 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/530.5 (KHTML, like Gecko) Chrome/2.0.173.1 Safari/530.5'}
#        proxies={"http": 'http://ejMSHe:DACqze@196.16.109.137:8000', "https": 'https://ejMSHe:DACqze@196.16.109.137:8000'}
        response = req.get(url, headers= headers)
    #    print(response.headers)
        res_html = BS(response.text, features="html.parser")
        res_html = res_html.body
        script = res_html.find('script', text=re.compile('.+sharedData.+'))
        script = script.text
        start_of_json = script.find('{')
        json_ob = script[start_of_json:-1]  # -1 means ignore the ending colon
        json_ob = json.loads(json_ob)
    
        followers= json_ob['entry_data']['ProfilePage'][0]['graphql']['user']['edge_followed_by']['count']
        following= json_ob['entry_data']['ProfilePage'][0]['graphql']['user']['edge_follow']['count']
        website= json_ob['entry_data']['ProfilePage'][0]['graphql']['user']['external_url']
        bio= json_ob['entry_data']['ProfilePage'][0]['graphql']['user']['biography']
        pic= json_ob['entry_data']['ProfilePage'][0]['graphql']['user']['profile_pic_url']
        email= re.findall(r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.[a-z]+", bio)
        user_id=json_ob['entry_data']['ProfilePage'][0]['graphql']['user']['id']
        fullname=json_ob['entry_data']['ProfilePage'][0]['graphql']['user']['full_name']
        is_verified=json_ob['entry_data']['ProfilePage'][0]['graphql']['user']['is_verified']
        is_private=json_ob['entry_data']['ProfilePage'][0]['graphql']['user']['is_private']
        media_count=json_ob['entry_data']['ProfilePage'][0]['graphql']['user']['edge_owner_to_timeline_media']['count']
        
        
        list_csv.append(user_id)
        list_csv.append(user)
        list_csv.append(fullname)
        list_csv.append(is_verified)
        list_csv.append(is_private)
        list_csv.append(media_count)
        list_csv.append(followers)
        list_csv.append(following)
        list_csv.append(bio)
        list_csv.append(website)
        list_csv.append(email)
        list_csv.append(pic)
        list_csv.append(0)
        list_csv.append('None')
        list_csv.append('www.instagram.com/'+ str(user))
        list_csv.append('Unknown')
        list_csv.append('Unknown')
        list_csv.append(t)
        list_csv.append(t)
        list_csv_all.append(list_csv)
        delays = [15,18, 20,22,27,30,35,40]
        delay = np.random.choice(delays)
        time.sleep(delay)
        

    except Exception as e:
        print(e)


#---------------Profile Details Database Update
        
df_list= pd.read_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/0-usernames-list.csv")
df= pd.read_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/3-details_predictions.csv")
#df=df.truncate(before=-1, after=-1)
l=list(df_list['username'])
df["gender_predicted"] = df.gender_predicted.astype(object)
#l1=l[8803:len(l)]
#l.index('abbierain.lky')
#df_profile= pd.read_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/1-user_tags.csv")#reading username list database
l2=list(df['username'])
#l1=l[8800: len(l)]

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




df2= pd.read_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/3-details_predictions.csv")
#df2=df2.truncate(before=-1, after=-1) #deleting all rows of the dataframe
i=len(df2)
for a in range(len(list_csv_all)):

    df2.at[i,'user_id']= list_csv_all[a][0]
    df2.at[i,'username']= list_csv_all[a][1]
    df2.at[i,'fullname']=list_csv_all[a][2]
    df2.at[i,'is_verified']=list_csv_all[a][3]
    df2.at[i,'is_private']=list_csv_all[a][4]
    df2.at[i,'media_count']=list_csv_all[a][5]
    df2.at[i,'follower_count']= list_csv_all[a][6]
    df2.at[i,'following_count']= list_csv_all[a][7]
    df2.at[i,'bio']= list_csv_all[a][8]
    df2.at[i,'website']= list_csv_all[a][9]
    df2.at[i,'emails']= list_csv_all[a][10]
    df2.at[i,'profile_pic']= list_csv_all[a][11]
    df2.at[i,'no_of_faces_detetcted']=list_csv_all[a][12]
    df2.at[i,'category_predicted']=list_csv_all[a][13]
    df2.at[i,'instagram_url']=list_csv_all[a][14]
    df2.at[i,'gender_predicted']=list_csv_all[a][15]
    df2.at[i,'age_predicted']=list_csv_all[a][16]
    df2.at[i,'created_date']= list_csv_all[a][17]
    df2.at[i,'updated_date']= list_csv_all[a][18]
    i=i+1

df2.to_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/3-details_predictions.csv", index=False)







df2= pd.read_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/3-details_predictions.csv")
df_list= pd.read_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/0-usernames-list.csv")
for index, row in df2.iterrows():
    user= df2.at[index,'username']
    print(user)
    if(df_list.loc[df_list['username'] == user, 'id'].iloc[0]):
        df2.at[index,'account_id']= df_list.loc[df_list['username'] == user, 'id'].iloc[0]

df2.to_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/3-details_predictions.csv", index=False)
#
    
      
df= pd.read_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/3-details_predictions.csv")
df.emails= df.emails.str.strip('[]')
df.emails= df.emails.str.strip("''")
df.to_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/3-details_predictions.csv",index=False)
##
###
###
##
data=pd.read_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/3-details_predictions.csv")
data['emails'].fillna('no_email', inplace=True)
data['website'].fillna('no_website', inplace=True)
data['fullname'].fillna('no_fullname', inplace=True) 
data['bio'].fillna('no_bio', inplace=True)     
data.to_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/3-details_predictions.csv', index=False)
##
####
####
####
####
####
####
from tqdm import tqdm
MODEL_MEAN_VALUES = (78.4263377603, 87.7689143744, 114.895847746)
age_list = ['(0, 2)', '(4, 6)', '(8, 12)', '(15, 20)', '(25, 32)', '(38, 43)', '(48, 53)', '(60, 100)']
gender_list = ['Male', 'Female']

def initialize_caffe_models():
	
	age_net = cv2.dnn.readNetFromCaffe(
		'/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/data/deploy_age.prototxt', 
		'/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/data/age_net.caffemodel')

	gender_net = cv2.dnn.readNetFromCaffe(
		'/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/data/deploy_gender.prototxt', 
		'/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/data/gender_net.caffemodel')

	return(age_net, gender_net)

def detect_face(age_net, gender_net, image,index):
    face_cascade = cv2.CascadeClassifier('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/data/haarcascade_frontalface_alt.xml')
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    faces = face_cascade.detectMultiScale(gray, 1.1, 5)
    no_of_faces_detected= len(faces)
    print(no_of_faces_detected)
    df3.at[index,'no_of_faces_detetcted']=no_of_faces_detected

    if(no_of_faces_detected==1):   
        for (x, y, w, h )in faces:
            cv2.rectangle(image, (x, y), (x+w, y+h), (255, 255, 0), 2)

            #Task 1 : Get Face 
            face_img = image[y:y+h, h:h+w].copy()
            blob = cv2.dnn.blobFromImage(face_img, 1, (227, 227), MODEL_MEAN_VALUES, swapRB=False)

			#Task 2 : Predict Gender
            gender_net.setInput(blob)
            gender_preds = gender_net.forward()
            gender = gender_list[gender_preds[0].argmax()]
            print(gender)
            df3.at[index,'gender_predicted']=gender
			#Task 3 : Predict Age
            age_net.setInput(blob)
            age_preds = age_net.forward()
            age = age_list[age_preds[0].argmax()]
            print(age)
            
            df3.at[index,'age_predicted']=age
#
#
#
df3=pd.read_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/3-details_predictions.csv')
for i in tqdm(range(0,len(df3))):
    try:
    
        url= df3.at[i,'profile_pic']
        resp = urllib.request.urlopen(url)
        image = np.asarray(bytearray(resp.read()), dtype="uint8")
        image = cv2.imdecode(image, cv2.IMREAD_COLOR)    
        age_net, gender_net = initialize_caffe_models()
        detect_face(age_net, gender_net, image, i)
        
    except Exception as e:
        print(e)
        
df3.to_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/3-details_predictions.csv', index=False)
###
####
####
#####
#def sum():
#    pbar = tqdm(total(os.listdir('path_of_file')))
#    for file in files:
#        pbar.set_description('remaining {i} files...)
#        multi_thread...
#        pbar.update(1)
####
####
####    
###    
df= pd.read_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/3-female_names.csv")    
list1=list(df['Female'])
list1 = [incom for incom in list1 if str(incom) != 'nan']    
df1= pd.read_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/3-details_predictions.csv")    
for index, row in df1.iterrows():
    fullname=row['fullname']
    username=row['username']
    fullname=str(fullname)
    username=str(username)
    fullname=fullname.lower()
    username=username.lower()
    
    name= username + fullname
    for ele in list1:
        if ele in name:
            print(ele)
            df1.at[index,'gender_predicted']='Female'
            
df1.to_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/3-details_predictions.csv',index=False)




df= pd.read_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/3-male_names.csv")    
list1=list(df['Male'])
list1 = [incom for incom in list1 if str(incom) != 'nan']    
df1= pd.read_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/3-details_predictions.csv")    
for index, row in df1.iterrows():
    fullname=row['fullname']
    username=row['username']
    fullname=str(fullname)
    username=str(username)
    fullname=fullname.lower()
    username=username.lower()
    
    name= username + fullname
    for ele in list1:
        if ele in name:
            print(ele)
            df1.at[index,'gender_predicted']='Male'
            
df1.to_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/3-details_predictions.csv',index=False)
####    
####    
####    
####    
####    
####    
####    
####    
####    
#####
#####Updating the Category based on email, no of faces for Brand/Product KOL/Human KOL and Normal User
####
df2=pd.read_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/3-details_predictions.csv')
for index, row in df2.iterrows():
    if(df2.iloc[index]['no_of_faces_detetcted']>=1):
        if(df2.iloc[index]['emails']=='no_email' and df2.iloc[index]['website']=='no_website'):
            df2.at[index,'category_predicted']='Normal User'
        else:
            df2.at[index,'category_predicted']='Human KOL'
    else:
        if(df2.iloc[index]['emails']=='no_email' or (df2.iloc[index]['is_verified']=='True' and df2.iloc[index]['emails']=='no_email')):
            df2.at[index,'category_predicted']='Brand'
        else:
            df2.at[index,'category_predicted']='Product KOL'
    df2.to_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/3-details_predictions.csv',index=False)
###    
####    
####    
#####----------DONE    
####    
####    
#from agefromname import AgeFromName
#import pandas as pd  #It includes data collected scraped from the Social Security Administration's Life Tables for the United States Social Security Area 1900-2100 and their baby names data. Code is included to re-scrape and refresh this data in regenerate_data.py. It includes data as far back as 1981.
#age_from_name = AgeFromName()
#
#
#df_p=pd.read_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/3-details_predictions.csv')
#
#for index,row in df_p[df_p.index > 370].iterrows():
#    a=row['username']
##input fullname or username of profile: if a substring contains a female name, script categorize to 'Female'
#
#    list2=[]
#    for a1 in a:
#        list2.append(a1)
#
#
#    count_f=0
#    count_m=0
#    total=0
#    for i in range(0,len(list2)):
#        list3=list2[:-i]
#        word=''.join(list3)
#        prob=age_from_name.prob_female(word)
#        total=total+prob
#        if(prob>0.5 and len(word)>3):
#            #print(word)
#            count_f=count_f+1
#        elif(prob<0.5 and len(word)>3):
#            #print(word)
#            count_m=count_m+1
#
#
#    for i in range(0,len(list2)):
#        list3=list2[i:]
#        word=''.join(list3)
#        #print(word)
#        prob=age_from_name.prob_female(word)
#        total=total+prob
#        if(prob>0.5 and len(word)>3):
#            #print(word)
#            count_f=count_f+1
#        elif(prob<0.5 and len(word)>3):
#            #print(word)
#            count_m=count_m+1
#
#
#    if(count_f>count_m):
#        df_p.at[index,'gender_predicted']='Female' 
#        print(a,'Female')
#    elif(count_m>count_f):
#        df_p.at[index,'gender_predicted']='Male'
#        print(a,'Male')
#
#    
#    df_p.to_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/3-details_predictions.csv',index=False)   
#
#
#
#
df=pd.read_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/3-details_predictions.csv')
df['country_origin'] = df.country_origin.astype(object)

list1= ['HK', 'hk', 'Hong Kong', 'HONG KONG', 'hong kong', '香港' ]
list2= ['Macau']
list3= ['USA', 'America']
list4= ['Canada', 'Canadian']
list5= ['Taiwan']

#df['country_origin']=''


for index,row in df.iterrows():
    for a in list1:
        if(a in row['bio']):
            df.at[index,'country_origin']='Hong Kong'
            
for index,row in df.iterrows():
    for a in list2:
        if(a in row['bio']):
            df.at[index,'country_origin']='Macau'


for index,row in df.iterrows():
    for a in list3:
        if(a in row['bio']):
            df.at[index,'country_origin']='America'
            
for index,row in df.iterrows():
    for a in list4:
        if(a in row['bio']):
            df.at[index,'country_origin']='Canada'
            
for index,row in df.iterrows():
    for a in list5:
        if(a in row['bio']):
            df.at[index,'country_origin']='Taiwan'
            
df.to_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/3-details_predictions.csv',index=False)


data=pd.read_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/3-details_predictions.csv")

data['country_origin'].fillna('Unknown', inplace=True)     
data.to_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/3-details_predictions.csv',index=False)
##
####    
#####

import pandas as pd
import re

df= pd.read_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/3-details_predictions.csv")

def pretty(s):
    if str(s)=='nan':
        return ''
    s = s.strip().replace('\r', '')
    s = re.sub(r' +', ' ', s)
    s = s.replace('\n', '\t')
    s = s.replace('\\n', '\t')
    s = s.replace('\\r', '')
    return s

print(df['bio'][2])
df['bio'] = df['bio'].map(pretty)


print(df['bio'][2])

df.to_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/3-details_predictions.csv", index=False)#writing tags to the csv file


#####
#####
##df= pd.read_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/3-details_predictions.csv")
##df["gender_predicted"] = df.gender_predicted.astype(object)
##print(df.dtypes)
##
##for index, row in df.iterrows():
##    gender=row['gender_predicted']
##    if(gender=='Male'):
##        df.at[index,'gender_predicted']=0
##    elif(gender=='Female'):
##        df.at[index,'gender_predicted']=1
##    elif(gender=='not_detected'):
##        df.at[index,'gender_predicted']=2
##    
##
##df.to_csv("/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/3-details_predictions.csv",index=False)
##
#
#




#####writing dataframe to mysql
####    
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

#cursor.execute("CREATE TABLE popared.ig_account_details \
#                (profile_id INT AUTO_INCREMENT NOT NULL,\
#                account_id INT,\
#                user_id VARCHAR(255), \
#                username VARCHAR(255), \
#                fullname nVARCHAR(255), \
#                is_verified VARCHAR(255), \
#                is_private VARCHAR(255), \
#                media_count INT, \
#                follower_count INT, \
#                following_count INT, \
#                bio nVARCHAR(500), \
#                website VARCHAR(255), \
#                emails VARCHAR(255), \
#                profile_pic VARCHAR(500),\
#                no_of_faces_detetcted INT,\
#                category_predicted VARCHAR(255),\
#                instagram_url VARCHAR(255),\
#                gender_predicted VARCHAR(255),\
#                age_predicted VARCHAR(255),\
#                created_date DATETIME,\
#                updated_date DATETIME,\
#                country_origin VARCHAR(255),\
#                FOREIGN KEY (account_id) REFERENCES ig_usernames(id),\
#                PRIMARY KEY (profile_id))")
# 

cursor.execute('TRUNCATE TABLE popared.ig_account_details')

cursor.execute("ALTER TABLE\
    ig_account_details\
    CONVERT TO CHARACTER SET utf8mb4\
    COLLATE utf8mb4_unicode_ci")


cursor.execute("LOAD DATA LOCAL INFILE '/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/3-details_predictions.csv' \
                INTO TABLE popared.ig_account_details \
                CHARACTER SET utf8mb4 \
                FIELDS TERMINATED BY ',' \
                OPTIONALLY ENCLOSED BY '\"'\
                LINES TERMINATED BY '\n' \
                IGNORE 1 LINES (user_id,username,fullname,is_verified,is_private,media_count,follower_count,following_count,bio,website,emails,profile_pic,no_of_faces_detetcted,category_predicted,instagram_url,gender_predicted,age_predicted,created_date,updated_date,account_id,country_origin)")
cursor.execute('ALTER TABLE ig_account_details MODIFY COLUMN updated_date date')  

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

#cursor.execute("CREATE TABLE popared.ig_account_details_history \
#                (profile_id INT AUTO_INCREMENT NOT NULL,\
#                account_id INT,\
#                user_id VARCHAR(255), \
#                username VARCHAR(255), \
#                fullname nVARCHAR(255), \
#                is_verified VARCHAR(255), \
#                is_private VARCHAR(255), \
#                media_count INT, \
#                follower_count INT, \
#                following_count INT, \
#                bio nVARCHAR(500), \
#                website VARCHAR(255), \
#                emails VARCHAR(255), \
#                profile_pic VARCHAR(500),\
#                no_of_faces_detetcted INT,\
#                category_predicted VARCHAR(255),\
#                instagram_url VARCHAR(255),\
#                gender_predicted VARCHAR(255),\
#                age_predicted VARCHAR(255),\
#                created_date DATETIME,\
#                updated_date DATETIME,\
#                country_origin VARCHAR(255),\
#                FOREIGN KEY (account_id) REFERENCES ig_usernames(id),\
#                PRIMARY KEY (profile_id))")
# 

cursor.execute('TRUNCATE TABLE popared.ig_account_details_history')

cursor.execute("ALTER TABLE\
    ig_account_details_history\
    CONVERT TO CHARACTER SET utf8mb4\
    COLLATE utf8mb4_unicode_ci")


cursor.execute("LOAD DATA LOCAL INFILE '/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/3-details_predictions.csv' \
                INTO TABLE popared.ig_account_details_history \
                CHARACTER SET utf8mb4 \
                FIELDS TERMINATED BY ',' \
                OPTIONALLY ENCLOSED BY '\"'\
                LINES TERMINATED BY '\n' \
                IGNORE 1 LINES (user_id,username,fullname,is_verified,is_private,media_count,follower_count,following_count,bio,website,emails,profile_pic,no_of_faces_detetcted,category_predicted,instagram_url,gender_predicted,age_predicted,created_date,updated_date,account_id,country_origin)")
cursor.execute('ALTER TABLE ig_account_details_history MODIFY COLUMN updated_date date')  

mydb.commit()
cursor.close()
print("Done")
