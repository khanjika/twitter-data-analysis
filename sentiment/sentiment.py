import pymongo 
import nltk
import csv

client=pymongo.MongoClient('mongodb+srv://B00843319:Khanjika@rawdb.uswer.mongodb.net/')
db = client['processedDb']
processedCollection = db.processedTweet


def readFile():
    with open('positive.txt', 'r') as positive, open('negative.txt', 'r') as negative:
        pWords = positive.read().split()
        nWords = negative.read().split()
    return pWords,nWords

def sentiment(pWords, nWords):
    with open('result.csv', 'w', newline='') as result:
        write = csv.writer(result)
        write.writerow(['Tweet', 'Message/Tweets', 'Match', 'Polarity'])
        counter = 1
        tweetCount = 1
        cursor = processedCollection.find()

        for tweet in cursor:
            tweetText = tweet['text']
            wordFreq = {}
            wordsInTweet = nltk.word_tokenize(tweetText)
            wordsInTweet = [words.lower() for words in wordsInTweet]
            
            for words in wordsInTweet:
                if words not in wordFreq.keys():
                    wordFreq[words] = 1
                else:
                    wordFreq[words] += 1

            positiveTweetWords = ''
            negativeTweetWords = ''
            neutral = ''

            for word in pWords:
                if word in wordFreq:
                    positiveTweetWords = positiveTweetWords + word + ' '

            for word in nWords:
                if word in wordFreq:
                    negativeTweetWords = negativeTweetWords + word + ' '


            if len(positiveTweetWords) > len(negativeTweetWords):
                write.writerow([tweetCount, tweetText, positiveTweetWords, 'positive'])
                counter +=1
            elif len(positiveTweetWords) < len(negativeTweetWords):
                write.writerow([tweetCount, tweetText, negativeTweetWords, 'negative'])
                counter +=1
            else:
                write.writerow([tweetCount, tweetText, neutral, 'neutral'])
                counter +=1
            tweetCount = tweetCount + 1
            	
            
if __name__ == "__main__":
   pWords,nWords =  readFile()
   sentiment(pWords,nWords)
    
    
			