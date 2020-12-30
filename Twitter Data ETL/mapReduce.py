import pymongo
from pyspark.sql import SparkSession
from pyspark import SparkContext

searchWords = ["storm","winter","canada","hot","cold","flu","snow","indoor","safety","rain","ice"]

#creating spark session
my_spark = SparkSession \
    .builder \
    .appName("wordCount") \
    .getOrCreate()

client = pymongo.MongoClient('mongodb+srv://B00843319:Khanjika@rawdb.uswer.mongodb.net/')
processedDb = client.processedDb
processedCollection = processedDb.processedTweet

reuterDb = client.reuterDb
collection = reuterDb.reuter

#loading news body from mongodb
newsBody = ""
for news in collection.find():
    newsBody += news["body"].lower()
    newsBody += " "      

#loading tweets text from mongodb
tweetText = ""
for tweets in processedCollection.find():
    tweetText += tweets["text"].lower()
    tweetText += " "

combinedText = newsBody + tweetText

def main():
        sc = SparkContext.getOrCreate()
        #split each line into words
        words = sc.parallelize([combinedText]).flatMap(lambda line: line.split(" "))
        # count the occurrence of each word
        wordCount = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)
	    #Sort the counts in descending order based on the word frequency
        sortedCounts =  wordCount.sortBy(lambda x: x[1], False)
        #iterate over the counts to print a word and its frequency
        #open file
        file = open("word_count.txt", 'w')
        for words,wordCount in sortedCounts.toLocalIterator():
            if words in searchWords:  
                #writing data to the file
                file.write(str(words)+":"+str(wordCount))
                file.write('\n')
    
if __name__ == "__main__":
    main()


    

