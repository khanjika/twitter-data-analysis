import pymongo
import tweepy as t
import json

client=pymongo.MongoClient('mongodb+srv://B00843319:Khanjika@rawdb.uswer.mongodb.net/')

mydb=client['rawDb']

search_words = ["Storm","Winter","Canada","Temperature","Flu","Snow","Indoor","Safety"]

api_key = "P5W438cyHHGZoxkpSJM4O2KUN";
api_secret_key = "dlWQgAGna8PANanNDFjZFcweCBLwDhDlOM3fAYEIHf2F5y6Cau";
access_token = "1324452533151191041-O3HUeb2kwm6g17ogWWBJKM2Ejl7hQO";
access_token_secret = "Yq78shbVcwpdBwfU77TKIT3LZITP0fTlbR532i2arw4e9";

auth = t.OAuthHandler(api_key, api_secret_key)
auth.set_access_token(access_token, access_token_secret)

def main():
#using twitter's search api
    api = t.API(auth, wait_on_rate_limit = True)
    print("You are now connected to the search API.")
    for word in search_words:
        tweets = t.Cursor(api.search,
                  tweet_mode='extended',
                    q=word,
                    ).items(300)
        #Using for loop,iterating for each tweet
        for tweet in tweets:
            id = tweet.id;
            username = tweet.user.screen_name
            created_at = tweet.created_at;
            text = tweet.full_text
            location = tweet.user.location;
            retweet_count = tweet.retweet_count;
            tweet = {'id':id, 'username':username, 'created_at':created_at, 'text':text, 'location':location, 'retweet_count': retweet_count}
            
            #insert the data into the mongoDB into a collection called rawTweets
            mydb.rawTweets.insert_one(tweet)
    print("Completed fetching tweets using search api") 

class StreamListener(t.StreamListener):    
    
    #limit the number of tweets obtain by streaming api to 2000 tweets
    def __init__(self):
        super().__init__()
        self.max_tweets = 2000
        self.tweet_count = 0

    def on_connect(self):
        # Called to connect to the Streaming API
        print("You are now connected to the streaming API.")
    
 
    def on_data(self, data):
        #connects to mongoDB and stores the tweet
        try:            
            try:
                data
            except TypeError:
                print("completed")
            else:
                self.tweet_count+=1
            if(self.tweet_count==self.max_tweets):
                print("Completed fetching streaming tweets")
                return(False)
            else:
    
                #Storing data from twitter into JSON
                datajson = json.loads(data)
            
                #fetching id
                id = datajson['id'];
                #fetching username
                username = datajson['user']['screen_name'];
                #fetching location
                location = datajson['user']['location'];
                #fetching retweet count
                retweet_count = datajson['retweet_count'];
                #fetching text
                text = datajson['text'];
                #fetching created_at
                created_at = datajson['created_at']            
            
            tweet = {'id':id, 'username':username, 'created_at':created_at, 'text':text, 'location':location, 'retweet_count': retweet_count}
            
            #insert the data into a collection called rawTweets
            mydb.rawTweets.insert_one(tweet)
        except Exception as e:
           print(e)

if __name__ == "__main__":
    #calling search api of twitter
    main()
    #calling strteaming api of twitter
    listener = StreamListener() 
    streamer = t.Stream(auth=auth, listener=listener, tweet_mode = 'extended', lang = 'en')
    streamer.filter(track=search_words)



           


