import pandas as pd
import datetime
import mysql.connector
import csv
import numpy as np

df= pd.read_csv("5-category-decider.csv")

l1=list(df['Food'])
l2=list(df['Beauty'])
l3=list(df['Travel'])
l4=list(df['Sports'])
l5=list(df['Fashion'])
l6=list(df['Art_Design'])
l7=list(df['Health_Fitness'])
l8=list(df['Technology'])
l9=list(df['News'])
l10=list(df['Comedy_Humor'])
l11=list(df['Pets'])
l12=list(df['Family_Parenting'])
l13=list(df['Music_Dance'])
l14=list(df['Home_Decor'])
l15=list(df['Business'])
l16=list(df['Entertainment'])
l17=list(df['Lifestyle'])






l1 = [incom for incom in l1 if str(incom) != 'nan']
l2 = [incom for incom in l2 if str(incom) != 'nan']
l3 = [incom for incom in l3 if str(incom) != 'nan']
l4 = [incom for incom in l4 if str(incom) != 'nan']
l5 = [incom for incom in l5 if str(incom) != 'nan']
l6 = [incom for incom in l6 if str(incom) != 'nan']
l7 = [incom for incom in l7 if str(incom) != 'nan']
l8 = [incom for incom in l8 if str(incom) != 'nan']
l9 = [incom for incom in l9 if str(incom) != 'nan']
l10 = [incom for incom in l10 if str(incom) != 'nan']
l11 = [incom for incom in l11 if str(incom) != 'nan']
l12 = [incom for incom in l12 if str(incom) != 'nan']
l13 = [incom for incom in l13 if str(incom) != 'nan']
l14 = [incom for incom in l14 if str(incom) != 'nan']
l15 = [incom for incom in l15 if str(incom) != 'nan']
l16 = [incom for incom in l16 if str(incom) != 'nan']
l17 = [incom for incom in l17 if str(incom) != 'nan']





df_tags= pd.read_csv("1-user_tags.csv") #for readings tags
df_profile= pd.read_csv("3-details_predictions.csv") #for reading bio
df_categories= pd.read_csv("5-category-detected.csv") #for reading category db
df_categories=df_categories.truncate(before=-1, after=-1)

df_list= pd.read_csv("0-usernames-list.csv")#reading username list database
l=list(df_list['username'])
df_categories["other_category_1"] = df_categories.other_category_1.astype(object)
df_categories["primary_category_2"] = df_categories.primary_category_2.astype(object)

l_tags=list(df_tags['username'])
l_bio=list(df_profile['username'])


for i in range(0, len(l)):
    list1=[]
    list2=[]
    user=l[i]
    print(user)
    
    if((user in l_tags) and (user in l_bio)):
    
    #tags
    
    
        df_categories.at[i,'username']=user
        if(df_tags.loc[df_tags['username'] == user, 'tags'].iloc[0]):
            tags_string= df_tags.loc[df_tags['username'] == user, 'tags'].iloc[0]
        else:
            tags_string='no_tags'
            
        print(tags_string)
        tags_string=tags_string.lower()
        
        for ele in l1:
            if ele in tags_string:
                list1.append('Food')
        for ele in l2:
            if ele in tags_string:
                list1.append('Beauty')
        for ele in l3:
            if ele in tags_string:
                list1.append('Travel')
        for ele in l4:
            if ele in tags_string:
                list1.append('Sports')
        for ele in l5:
            if ele in tags_string:
                list1.append('Fashion')
        for ele in l6:
            if ele in tags_string:
                list1.append('Art_Design')
        for ele in l7:
            if ele in tags_string:
                list1.append('Health_Fitness')
        for ele in l8:
            if ele in tags_string:
                list1.append('Technology')
        for ele in l9:
            if ele in tags_string:
                list1.append('News')
        for ele in l10:
            if ele in tags_string:
                list1.append('Comedy_Humor')
        for ele in l11:
            if ele in tags_string:
                list1.append('Pets')
        for ele in l12:
            if ele in tags_string:
                list1.append('Family_Parenting')
        for ele in l13:
            if ele in tags_string:
                list1.append('Music_Dance')
        for ele in l14:
            if ele in tags_string:
                list1.append('Home_Decor')
        for ele in l15:
            if ele in tags_string:
                list1.append('Business')
        for ele in l16:
            if ele in tags_string:
                list1.append('Entertainment')
        for ele in l17:
            if ele in tags_string:
                list1.append('Lifestyle')
                
                
        
        dic1= dict((x,list1.count(x)) for x in set(list1))
        print("tags: ",dic1)
        
        
        
        
        
        
        
        #bio and fullname
        
        if(df_profile.loc[df_profile['username'] == user, 'bio'].iloc[0]):
            bio_string= df_profile.loc[df_profile['username'] == user, 'bio'].iloc[0]
        else:
            bio_string='no_bio'
        
        if(df_profile.loc[df_profile['username'] == user, 'fullname'].iloc[0]):
            fullname_string= df_profile.loc[df_profile['username'] == user, 'fullname'].iloc[0]
        else:
            fullname_string='no_fullname'
        
        
        s_string= str(bio_string) + str(fullname_string)
        s_string=s_string.lower()
    
            
        for ele in l1:
            if ele in s_string:
                list2.append('Food')
        for ele in l2:
            if ele in s_string:
                print(ele)
                list2.append('Beauty')
        for ele in l3:
            if ele in s_string:
                list2.append('Travel')
        for ele in l4:
            if ele in s_string:
                list2.append('Sports')
        for ele in l5:
            if ele in s_string:
                list2.append('Fashion')
        for ele in l6:
            if ele in s_string:
                list2.append('Art_Design')
        for ele in l7:
            if ele in s_string:
                list2.append('Health_Fitness')
        for ele in l8:
            if ele in s_string:
                list2.append('Technology')
        for ele in l9:
            if ele in s_string:
                list2.append('News')
        for ele in l10:
            if ele in s_string:
                list2.append('Comedy_Humor')
        for ele in l11:
            if ele in s_string:
                list2.append('Pets')
        for ele in l12:
            if ele in s_string:
                list2.append('Family_Parenting')
        for ele in l13:
            if ele in s_string:
                list2.append('Music_Dance')
        for ele in l14:
            if ele in s_string:
                list2.append('Home_Decor')
        for ele in l15:
            if ele in s_string:
                list2.append('Business')
        for ele in l16:
            if ele in s_string:
                list2.append('Entertainment')
        for ele in l17:
            if ele in s_string:
                list2.append('Lifestyle')
                
              
        dic2= dict((x,list2.count(x)) for x in set(list2))
        print("bio+fullname: ",dic2)
    
        for key in dic2: 
            if key in dic1: 
                dic2[key] = dic2[key] + dic1[key] 
                del dic1[key]
             
        dic2.update(dic1) 
        print('total: ', dic2)
        
        list3=[]#primary category
        list4=[]#other categories
        
        if(dic2):
            primary_cat= max(dic2.values()) # maximum frequency of any category
            for key, value in dic2.items():    
                if(value==primary_cat):
                    list3.append(key)#list of keys with maximum value in the dictionary
                    
        for key, value in dic2.items():
            if(key not in list3):
                list4.append(key)#keys not in list3
        
        print('Primary Category ', list3) #list of primary categories
        print('Other Categories ', list4) #list of remaining categories
        
        if(len(list3)==1):
            df_categories.at[i,'primary_category_0']=str(list3[0])
        
        if(len(list3)==2):
            df_categories.at[i,'primary_category_0']=str(list3[0])
            df_categories.at[i,'primary_category_1']=str(list3[1])
            
            
        if(len(list3)>=3):
            df_categories.at[i,'primary_category_0']=str(list3[0])
            df_categories.at[i,'primary_category_1']=str(list3[1])
            df_categories.at[i,'primary_category_2']=str(list3[2])
            
               
            
        if(len(list4)==1):
            df_categories.at[i,'other_category_0']=str(list4[0]) 
            
        if(len(list4)>=2):
            df_categories.at[i,'other_category_0']=str(list4[0])
            df_categories.at[i,'other_category_1']=str(list4[1])
            
            
            
        if(df_list.loc[df_list['username'] == user, 'id'].iloc[0]):
            df_categories.at[i,'account_id']= df_list.loc[df_list['username'] == user, 'id'].iloc[0]
         
            
        now= datetime.datetime.now()
        t= now.strftime("%Y-%m-%d %H:%M") 
        
        df_categories.at[i,'created_date']= t
        df_categories.at[i,'updated_date']= t
        print('-------------------')
        
    
        
        df_categories.to_csv('5-category-detected.csv',index=False)
#    
#
#
#
df_categories= pd.read_csv("5-category-detected.csv") #for reading category db
df_categories['primary_category_0'].fillna('Lifestyle', inplace=True)
df_categories.to_csv('5-category-detected.csv',index=False)
#

df_categories= pd.read_csv("5-category-detected.csv")
df_categories= df_categories[['account_id','username', 'primary_category_0', 'primary_category_1', 'primary_category_2','other_category_0', 'other_category_1','created_date','updated_date']]
df_categories.to_csv('5-category-detected.csv',index=False)

###
#
##

##
###
###
#####writing csv file to mysqldb
####
#mydb = mysql.connector.connect(
#  host='113.52.134.24',
#  database='popared',
#  user='popared',
#  passwd='Iampopared2019'
#)
#cursor = mydb.cursor()
#
#
#data_path = '//Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/5-category-detected.csv'
#with open(data_path, 'r') as f:
#    reader = csv.reader(f, delimiter=',')
#    next(reader, None)  # skip the headers
#
#    for row in reader:
#        cursor.execute('INSERT INTO popared.ig_categories(username, primary_category_0, primary_category_1, \
#        primary_category_2, other_category_0, other_category_1 , created_date,updated_date, account_id) \
#                       VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)', \
#                       (row[0], row[4], row[5], row[6], row[7], row[8], row[1],row[2], row[3]))
#
#            
#
#mydb.commit()
#cursor.close()
#print("Done")
##
###
###
###
###
###
mydb = mysql.connector.connect(
  host='113.52.134.24',
  database='popared',
  user='popared',
  passwd='Iampopared2019'
)
cursor = mydb.cursor()

#cursor.execute("CREATE TABLE popared.ig_categories \
#                 (cat_id INT AUTO_INCREMENT NOT NULL,\
#                  account_id INT,\
#                  username VARCHAR(255), \
#                  primary_category_0 VARCHAR(255), \
#                  primary_category_1 VARCHAR(255), \
#                  primary_category_2 VARCHAR(255), \
#                  other_category_0 VARCHAR(255), \
#                  other_category_1 VARCHAR(255), \
#                  created_date DATETIME, \
#                  updated_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, \
#                  FOREIGN KEY (account_id) REFERENCES usernames(id),\
#                  PRIMARY KEY (cat_id))")

cursor.execute('TRUNCATE TABLE popared.ig_categories')

cursor.execute("LOAD DATA LOCAL INFILE '/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/5-category-detected.csv' \
                INTO TABLE popared.ig_categories \
                CHARACTER SET utf8mb4 \
                FIELDS TERMINATED BY ',' \
                LINES TERMINATED BY '\n' \
                IGNORE 1 LINES (account_id,username, primary_category_0, primary_category_1, primary_category_2,other_category_0, other_category_1,created_date,updated_date)")
cursor.execute('ALTER TABLE ig_categories MODIFY COLUMN updated_date date')
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

#cursor.execute("CREATE TABLE popared.ig_categories_history \
#                 (cat_id INT AUTO_INCREMENT NOT NULL,\
#                  account_id INT,\
#                  username VARCHAR(255), \
#                  primary_category_0 VARCHAR(255), \
#                  primary_category_1 VARCHAR(255), \
#                  primary_category_2 VARCHAR(255), \
#                  other_category_0 VARCHAR(255), \
#                  other_category_1 VARCHAR(255), \
#                  created_date DATETIME, \
#                  updated_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, \
#                  FOREIGN KEY (account_id) REFERENCES usernames(id),\
#                  PRIMARY KEY (cat_id))")


cursor.execute("LOAD DATA LOCAL INFILE '/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/5-category-detected.csv' \
                INTO TABLE popared.ig_categories_history \
                CHARACTER SET utf8mb4 \
                FIELDS TERMINATED BY ',' \
                LINES TERMINATED BY '\n' \
                IGNORE 1 LINES (account_id,username, primary_category_0, primary_category_1, primary_category_2,other_category_0, other_category_1,created_date,updated_date)")
cursor.execute('ALTER TABLE ig_categories_history MODIFY COLUMN updated_date date')
mydb.commit()
cursor.close()
print("Done")




#####
####
###
###
#mydb = mysql.connector.connect(
#  host='113.52.134.24',
#  database='popared',
#  user='popared',
#  passwd='Iampopared2019'
#)
#cursor = mydb.cursor()
#
#
#data_path = '//Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/5-category-detected.csv'
#with open(data_path, 'r') as f:
#    reader = csv.reader(f, delimiter=',')
#    next(reader, None)  # skip the headers
#    ind=0
#    for row in reader:
#        cursor.execute('INSERT INTO popared.categories_history(username, primary_category_0, primary_category_1, \
#        primary_category_2, other_category_0, other_category_1 , created_date, account_id) \
#                       VALUES(%s,%s,%s,%s,%s,%s,%s,%s)', \
#                       (row[1], row[5], row[6], row[7], row[8], row[9], row[2], row[4]))
#        ind=ind+1
#        if(ind>len(df)):
#            break
#            
#
#mydb.commit()
#cursor.close()
#print("Done")


