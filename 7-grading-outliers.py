import pandas as pd
import math
import random
import datetime
import numpy as np
from sklearn.preprocessing import normalize
from scipy import stats
from sklearn import preprocessing

def remove_outlier(df_in, col_name):
    q1 = df_in[col_name].quantile(0.05)
    q3 = df_in[col_name].quantile(0.95)
    df_out = df_in.loc[(df_in[col_name] > q1) & (df_in[col_name] < q3)]
    return df_out

df1= pd.read_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/3-details_predictions.csv')
df2=pd.read_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/4-details_website.csv')

df_out11= remove_outlier(df1, 'follower_count')
df_out12= remove_outlier(df1, 'media_count')
df_out1 = pd.merge(df_out11, df_out12, how='inner')
print(len(df_out1))
df_out111= remove_outlier(df2, 'engagement_rate')
df_out121= remove_outlier(df2, 'average_likes')
df_out2 = pd.merge(df_out111, df_out121, how='inner')
print(len(df_out2))

cols_to_norm = ['media_count','follower_count']
df_out1[cols_to_norm] = df_out1[cols_to_norm].apply(lambda x: (x - x.min()) / (x.max() - x.min()))

cols_to_norm2 = ['engagement_rate', 'average_likes']
df_out2[cols_to_norm2] = df_out2[cols_to_norm2].apply(lambda x: (x - x.min()) / (x.max() - x.min()))


df_list= pd.read_csv("0-usernames-list.csv")
df4=pd.read_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/7-grade_score.csv')
df4=df4.truncate(before=-1, after=-1)
l=list(df_list['username'])
df4[['grade']] = df4[['grade']].astype('object')

for i in range(0,len(l)):
    try:

        user=l[i]
    
        if(len(df_out1[df_out1['username']==user]['media_count'].values)>=1 and len(df_out1[df_out1['username']==user]['follower_count'].values)>=1 and len(df_out2[df_out2['username']==user]['engagement_rate'].values)>=1):
            media_count= df_out1.loc[df_out1['username'] == user, 'media_count'].values[0]
            follow= df_out1.loc[df_out1['username'] == user, 'follower_count'].values[0]
            eng_rat= df_out2.loc[df_out2['username'] == user, 'engagement_rate'].values[0]
            avg_likes= df_out2.loc[df_out2['username'] == user, 'average_likes'].values[0]
            
            print(media_count, follow, eng_rat, avg_likes)
            score= 0.2*media_count + 0.4*follow + 0.1 * eng_rat + 0.3*avg_likes
            score=score*100
            score= round(score)
            df4.at[i,'username']=user
            df4.at[i,'score']=score
            if(score>=60):
                df4.at[i,'grade']='A+'
            elif(score>=50 and score<60):
                df4.at[i,'grade']='A'
            elif(score>=30 and score<50):
                df4.at[i,'grade']='B+'
            elif(score>=10 and score<30):
                df4.at[i,'grade']='B'
            else:
                df4.at[i,'grade']='C'
                df4.to_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/7-grade_score.csv',index=False)
        else:
            if(len(df1[df1['username']==user]['follower_count'].values)>=1):
        
                follow=df1[df1['username']==user]['follower_count'].values
                v1= df1["follower_count"].quantile(0.6)
                v2=df1["follower_count"].quantile(0.3)
                v3=df1["follower_count"].quantile(0.2)
                if(follow>v1):
                    df4.at[i,'username']=user
                    df4.at[i,'score']=-1
                    df4.at[i,'grade']='A+'
                elif(follow> v2 and follow <= v1):
                    df4.at[i,'username']=user
                    df4.at[i,'score']=-1
                    df4.at[i,'grade']='B+'
                elif(follow>v3 and follow <= v2):
                    df4.at[i,'username']=user
                    df4.at[i,'score']=-1
                    df4.at[i,'grade']='B'
                else:
                    df4.at[i,'username']=user
                    df4.at[i,'score']=-2
                    df4.at[i,'grade']='D'
                df4.to_csv('/Users/PopaRED2/Desktop/Priya Mehrotra/Instagram-Follower-Scraper/7-grade_score.csv',index=False)
            else:
                print('no information available of the user')
    
    except Exception as e:
        print(e)

        
