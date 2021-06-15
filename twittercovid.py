# -*- coding: utf-8 -*-
"""
Created on Mon May  4 02:11:25 2020

@author: ASUS
"""
import twython
from twython import Twython
import pandas as pd
import glob, os
import random
import pymongo	
from pymongo import MongoClient, InsertOne
import time

os.chdir('D:/OneDrive/0.FTD/Applied data science 2/COVID-19-TweetIDs-master/data2')
client = MongoClient('localhost', 27017)
#client = MongoClient('mongodb+srv://twittercovid:covid19@twittercovid-fzsas.mongodb.net/test?retryWrites=true&w=majority')
db=client['Sorbonne']

authorized = 0
#if you want to run the code, change run = 1 and change line 18 to line 19 or line 16 to 17

#%% Extract twitter IDs and import in Mongodb
if authorized == 1: 
    file_count=0
    TweetIDs_list = []   
    while file_count<100:
             
        file = random.choice(glob.glob("*.txt"))
        print(file,"-----------file_count=",file_count)
        
        with open(file) as f_in:
            for line in f_in:                
                TweetIDs_list.append(line.rstrip())
        f_in.close()
        address1 = 'D:/OneDrive/0.FTD/Applied data science 2/COVID-19-TweetIDs-master/data2/'
        address2 = 'D:/OneDrive/0.FTD/Applied data science 2/COVID-19-TweetIDs-master/scrapped/'
        os.rename(address1+file,address2+file)
        file_count +=1

#%% scrape data from Twitter, extract text from IDs
# Write the tweet elements in mongo database

#______________________________ BOT 1 ________________________________
if authorized == 2: 
    while len(TweetIDs_list)>100:
        try: 
            APP_KEY='S615vEwWEnYt4opmB9BTyCmvl'
            APP_SECRET='iVMnlPMcpCRKzKFUMIxyWHYdnpKajYlyKrcgSjUpeqqtwXDF6r'
            OAUTH_TOKEN='1097861033568468993-9rAVyXhMLXD4jVxCvyB7X5u169LzZL'
            OAUTH_TOKEN_SECRET='LLFWHRBf41Kk7WMgevlpP0XM1d8jANDxYKJv7CM7cQZ6w'
            twitter = Twython(APP_KEY,APP_SECRET,OAUTH_TOKEN,OAUTH_TOKEN_SECRET)
            if authorized == 2: 
                data_entries=0
                while data_entries<len(TweetIDs_list)+1:
                    ids=[]
                    ids= TweetIDs_list[data_entries:data_entries+100]
                    TweetIDs_list=TweetIDs_list[len(ids):len(TweetIDs_list)]
                        
                    tweets = twitter.lookup_status(id=",".join(ids))
                
                    for tweet in tweets:
                        hashtag_list=[]
                        hashtag_display=""
                        for h in tweet["entities"]["hashtags"]:
                            hashtag_list.append(h['text'])
                            hashtag_display=','.join(hashtag_list)
                        insert_tweet = {"ID": tweet["id"], "TimeStamp": tweet["created_at"], "User": tweet["user"]["screen_name"], "UserID": tweet["user"]["id"], "Content": tweet["text"],"Hashtag": hashtag_list,"Hashtag_display": hashtag_display, "Location": tweet["user"]["location"], "Follower": tweet["user"]["followers_count"], "Friends": tweet["user"]["friends_count"],"StatusCount": tweet["user"]["statuses_count"],"Retweets": tweet["retweet_count"],"Favorites": tweet["favorite_count"],"Language" : tweet["lang"]}
                        db["Tweets_data"].insert_one(insert_tweet)
                    data_entries+=100
                    print("progress:",data_entries," tweets out of ", len(TweetIDs_list))
        except ValueError:
            APP_KEY='fSKUBuChab85ELD2Nx7geSR3r'
            APP_SECRET='ZjS5QLp0NSVmzoxaXlyI6GFEDOl3sNX2cppH3MX6RT4y2vFRVU'
            OAUTH_TOKEN='1097861033568468993-ptXG4NEqbbGKHh57iXVYnuKRHFbk9t'
            OAUTH_TOKEN_SECRET='W2iLJLL38ni0kpnhe5brfe5mu8a7oehu2g4CJoOlxYpkX'
            twitter = Twython(APP_KEY,APP_SECRET,OAUTH_TOKEN,OAUTH_TOKEN_SECRET)
            if authorized == 2: 
                data_entries=0
                while data_entries<len(TweetIDs_list)+1:
                    ids=[]
                    ids= TweetIDs_list[data_entries:data_entries+100]
                    TweetIDs_list=TweetIDs_list[len(ids):len(TweetIDs_list)]
                        
                    tweets = twitter.lookup_status(id=",".join(ids))
                
                    for tweet in tweets:
                        hashtag_list=[]
                        hashtag_display=""
                        for h in tweet["entities"]["hashtags"]:
                            hashtag_list.append(h['text'])
                            hashtag_display=','.join(hashtag_list)
                        insert_tweet = {"ID": tweet["id"], "TimeStamp": tweet["created_at"], "User": tweet["user"]["screen_name"], "UserID": tweet["user"]["id"], "Content": tweet["text"],"Hashtag": hashtag_list,"Hashtag_display": hashtag_display, "Location": tweet["user"]["location"], "Follower": tweet["user"]["followers_count"], "Friends": tweet["user"]["friends_count"],"StatusCount": tweet["user"]["statuses_count"],"Retweets": tweet["retweet_count"],"Favorites": tweet["favorite_count"],"Language" : tweet["lang"]}
                        db["Tweets_data"].insert_one(insert_tweet)
                    data_entries+=100
                    print("progress:",data_entries," tweets out of ", len(TweetIDs_list))
        except:
            time.sleep(300)
            continue

#%%
#______________________________ BOT 2 ________________________________
if authorized == 2: 
    while len(TweetIDs_list)>100:
        try:
            APP_KEY='Y3jIm3CAdfPXNiv2ptsz0weWt'
            APP_SECRET='3Tk7zfBMN8CQXYIk8sO02J785v1WUO0KBLM047zqsNyvlIfqCV'
            OAUTH_TOKEN='1097861033568468993-JCJw4sIk65tYuaK7oeDjGGsCqmeEjL'
            OAUTH_TOKEN_SECRET='iL7E3lLoL8tpNAq7v5QT3988iLOSs0yXxJJdaxVaMhOR7'
            twitter = Twython(APP_KEY,APP_SECRET,OAUTH_TOKEN,OAUTH_TOKEN_SECRET)
            if authorized == 2: 
                data_entries=0
                while data_entries<len(TweetIDs_list)+1:
                    ids=[]
                    ids= TweetIDs_list[data_entries:data_entries+100]
                    TweetIDs_list=TweetIDs_list[len(ids):len(TweetIDs_list)]
                        
                    tweets = twitter.lookup_status(id=",".join(ids))
                
                    for tweet in tweets:
                        hashtag_list=[]
                        hashtag_display=""
                        for h in tweet["entities"]["hashtags"]:
                            hashtag_list.append(h['text'])
                            hashtag_display=','.join(hashtag_list)
                        insert_tweet = {"ID": tweet["id"], "TimeStamp": tweet["created_at"], "User": tweet["user"]["screen_name"], "UserID": tweet["user"]["id"], "Content": tweet["text"],"Hashtag": hashtag_list,"Hashtag_display": hashtag_display, "Location": tweet["user"]["location"], "Follower": tweet["user"]["followers_count"], "Friends": tweet["user"]["friends_count"],"StatusCount": tweet["user"]["statuses_count"],"Retweets": tweet["retweet_count"],"Favorites": tweet["favorite_count"],"Language" : tweet["lang"]}
                        db["Tweets_data"].insert_one(insert_tweet)
                    data_entries+=100
                    print("progress:",data_entries," tweets out of ", len(TweetIDs_list))
        except ValueError:
            APP_KEY='JwTbY8shofhWc5kCWHsjeUQfo'
            APP_SECRET='GXlUkJ8RatNtaSMVEo4bf3c5sosfA6ZMEYDFKdiXvG0phtlVCF'
            OAUTH_TOKEN='1097861033568468993-SVc4SdEX0WOrj6xGQVtBUi8VNnH2DL'
            OAUTH_TOKEN_SECRET='P7eHPSmLFLPgcixGHpcCVsrZ4FmtySOzSKu2pBZ1IalQl'
            twitter = Twython(APP_KEY,APP_SECRET,OAUTH_TOKEN,OAUTH_TOKEN_SECRET)
            if authorized == 2: 
                data_entries=0
                while data_entries<len(TweetIDs_list)+1:
                    ids=[]
                    ids= TweetIDs_list[data_entries:data_entries+100]
                    TweetIDs_list=TweetIDs_list[len(ids):len(TweetIDs_list)]
                        
                    tweets = twitter.lookup_status(id=",".join(ids))
                
                    for tweet in tweets:
                        hashtag_list=[]
                        hashtag_display=""
                        for h in tweet["entities"]["hashtags"]:
                            hashtag_list.append(h['text'])
                            hashtag_display=','.join(hashtag_list)
                        insert_tweet = {"ID": tweet["id"], "TimeStamp": tweet["created_at"], "User": tweet["user"]["screen_name"], "UserID": tweet["user"]["id"], "Content": tweet["text"],"Hashtag": hashtag_list,"Hashtag_display": hashtag_display, "Location": tweet["user"]["location"], "Follower": tweet["user"]["followers_count"], "Friends": tweet["user"]["friends_count"],"StatusCount": tweet["user"]["statuses_count"],"Retweets": tweet["retweet_count"],"Favorites": tweet["favorite_count"],"Language" : tweet["lang"]}
                        db["Tweets_data"].insert_one(insert_tweet)
                    data_entries+=100
                    print("progress:",data_entries," tweets out of ", len(TweetIDs_list))
        except:
            time.sleep(300)
            continue


#%%
#______________________________ BOT 3 ________________________________
if authorized == 2: 
    while len(TweetIDs_list)>100:
        try:
            APP_KEY='eK22vtg5hB3U9oXSI5TUCV1tK'
            APP_SECRET='kIp9eqcVNC3dNxL6i989OxrUmf8p4bZtqAmjUHLyBFbIYlPKZk'
            OAUTH_TOKEN='1097861033568468993-2p36ohEyIIdyRauPNPMHiE1eFWC7DB'
            OAUTH_TOKEN_SECRET='PDMx3SzwSbDyL0JJkmx9RAGutk9AJmUUmViRIZudRs0mf'
            twitter = Twython(APP_KEY,APP_SECRET,OAUTH_TOKEN,OAUTH_TOKEN_SECRET)
            if authorized == 2: 
                data_entries=0
                while data_entries<len(TweetIDs_list)+1:
                    ids=[]
                    ids= TweetIDs_list[data_entries:data_entries+100]
                    TweetIDs_list=TweetIDs_list[len(ids):len(TweetIDs_list)]
                        
                    tweets = twitter.lookup_status(id=",".join(ids))
                
                    for tweet in tweets:
                        hashtag_list=[]
                        hashtag_display=""
                        for h in tweet["entities"]["hashtags"]:
                            hashtag_list.append(h['text'])
                            hashtag_display=','.join(hashtag_list)
                        insert_tweet = {"ID": tweet["id"], "TimeStamp": tweet["created_at"], "User": tweet["user"]["screen_name"], "UserID": tweet["user"]["id"], "Content": tweet["text"],"Hashtag": hashtag_list,"Hashtag_display": hashtag_display, "Location": tweet["user"]["location"], "Follower": tweet["user"]["followers_count"], "Friends": tweet["user"]["friends_count"],"StatusCount": tweet["user"]["statuses_count"],"Retweets": tweet["retweet_count"],"Favorites": tweet["favorite_count"],"Language" : tweet["lang"]}
                        db["Tweets_data"].insert_one(insert_tweet)
                    data_entries+=100
                    print("progress:",data_entries," tweets out of ", len(TweetIDs_list))
        except ValueError:
        
            APP_KEY='mvBDtvdqRtpsm4ZzCU9EBBhLS'
            APP_SECRET='Gg3TVSbGrnl7g59MCnOkGmZMS3dUmzwjBbhljqkWMv1J1BSYdO'
            OAUTH_TOKEN='1097861033568468993-O3tfFcGJUytgJbGuJYXdMX9j5a7V28'
            OAUTH_TOKEN_SECRET='aOV2sctSzFBhoeVkWqraYDUq55vKcmcZGuwAGGvYpk1hy'
            twitter = Twython(APP_KEY,APP_SECRET,OAUTH_TOKEN,OAUTH_TOKEN_SECRET)
            if authorized == 2: 
                data_entries=0
                while data_entries<len(TweetIDs_list)+1:
                    ids=[]
                    ids= TweetIDs_list[data_entries:data_entries+100]
                    TweetIDs_list=TweetIDs_list[len(ids):len(TweetIDs_list)]
                        
                    tweets = twitter.lookup_status(id=",".join(ids))
                
                    for tweet in tweets:
                        hashtag_list=[]
                        hashtag_display=""
                        for h in tweet["entities"]["hashtags"]:
                            hashtag_list.append(h['text'])
                            hashtag_display=','.join(hashtag_list)
                        insert_tweet = {"ID": tweet["id"], "TimeStamp": tweet["created_at"], "User": tweet["user"]["screen_name"], "UserID": tweet["user"]["id"], "Content": tweet["text"],"Hashtag": hashtag_list,"Hashtag_display": hashtag_display, "Location": tweet["user"]["location"], "Follower": tweet["user"]["followers_count"], "Friends": tweet["user"]["friends_count"],"StatusCount": tweet["user"]["statuses_count"],"Retweets": tweet["retweet_count"],"Favorites": tweet["favorite_count"],"Language" : tweet["lang"]}
                        db["Tweets_data"].insert_one(insert_tweet)
                    data_entries+=100
                    print("progress:",data_entries," tweets out of ", len(TweetIDs_list))
        except:
            time.sleep(300)
            continue

