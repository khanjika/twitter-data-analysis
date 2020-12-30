import pymongo
import nltk
import csv
import math

client = pymongo.MongoClient('mongodb+srv://B00843319:Khanjika@rawdb.uswer.mongodb.net/')
mydb = client.reuterDb
collection = mydb.reuter


def frequencyInverse():
    cursor = collection.find()
    documentCount = cursor.count()
    searchWords = ['canada', 'rain', 'cold', 'hot']


    with open('result.csv', 'w', newline='') as result:
        searchWordCount = {'canada': 0, 'rain': 0, 'cold': 0, 'hot': 0}
        #opening the csv file, and writting the column heading
        write = csv.writer(result)
        write.writerow(['Total Documents', documentCount])
        write.writerow(
            ['Search Query', 'Document containing term(df)', 'Total Documents(N)/number of documents term appeared(df)',
             'Log10(N/df)'])

        #calculating the documents containing the search words
        for news in cursor:
            newsBody = news['body']
            newsTitle = news['title']
            newsBody = nltk.word_tokenize(newsBody.lower());
            newsTitle = nltk.word_tokenize(newsTitle.lower());
            for word in searchWords:
                if word in newsBody or word in newsTitle:
                    searchWordCount[word] += 1

        #writing the output to the file
        for word in searchWords:
            if (searchWordCount[word] > 0):
                write.writerow([word, searchWordCount[word], (documentCount / searchWordCount[word]),
                                math.log10(documentCount / searchWordCount[word])])
        return searchWordCount

def frequencyCount(searchWordCount):
    counter = 0
    relativeFrequency = 0
    newsData = {'title': '', 'body': ''}
    with open('canada_count.csv', 'w', newline='') as canada:
        #opening the csv file, and writting the column heading
        writeFile = csv.writer(canada)
        writeFile.writerow(['Term', 'Canada'])
        writeFile.writerow(
            [('Canada appeared in ' + str(searchWordCount["canada"]) + ' documents'), 'Total	Words (m)', 'Frequency (f)'])
        cursor = collection.find()
        for news in cursor:
            totalWords = 0
            count = 0
            newsTitle = nltk.word_tokenize(str(news['title']).lower())
            newsBody = nltk.word_tokenize(str(news['body']).lower())
            totalWords = len(newsTitle) + len(newsBody)
            for word in newsTitle:
                if word == "canada":
                    count += 1
            for word in newsBody:
                if word == "canada":
                    count += 1
            if(count > 0):
                writeFile.writerow(['Article #'+str(counter), totalWords, count])
                counter += 1
               
            
            if totalWords > 0:
                if count / totalWords > relativeFrequency:
                    relativeFrequency = count / totalWords
                    newsData['title'] = news['title']
                    newsData['body'] = news['body']
        return relativeFrequency,newsData
    
    
def highestRelativeFrequency(relativeFrequency,newsData):       
        print('\n')
        print('News article with highest relative frequency')
        print('Highest Relative Frequency: ' + str(relativeFrequency))
        print('News title: ' + newsData['title'])
        print('News description: ' + newsData['body'])
                

if __name__ == "__main__":
    searchWordCount = frequencyInverse()
    relativeFrequency,newsData = frequencyCount(searchWordCount)
    highestRelativeFrequency(relativeFrequency,newsData)
    
			