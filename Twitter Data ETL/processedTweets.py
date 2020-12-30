import re
import pymongo 

client = pymongo.MongoClient('mongodb+srv://B00843319:Khanjika@rawdb.uswer.mongodb.net/')
rawdDb = client.rawDb
rawCollection = rawdDb.rawTweets

processedDb = client.processedDb
processedCollection = processedDb.processedTweet

def main():
    for tweet in rawCollection.find():
        
        clean_record = tweet
        #cleaning text of tweets
        text = tweet['text']
	    #following regex removes emojis and othe special characters
        emoji_pattern = re.compile("["	
                         u"\U0001F600-\U0001F64F"  # emoticons
                         u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                         u"\U0001F680-\U0001F6FF"  # transport & map symbols
                         u"\U0001F1E0-\U0001F1FF"  # flags (iOS) 
               	    	 u"\U00002702-\U000027B0"
                   		 u"\U000024C2-\U0001F251"
                   		"]+", flags=re.UNICODE)
        text = emoji_pattern.sub(r'', text)
        text = re.sub(r"\s+"," ", text, flags = re.I) # remove multiple spaces
        text = re.sub(r"[,@\'?\.$%_]", "", text, flags=re.I) # grouping pattern
        
        
        clean_record['text'] = text
        processedCollection.insert_one(clean_record)
    print("Completed")

if __name__ == "__main__":
    main()