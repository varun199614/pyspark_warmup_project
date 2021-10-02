import sys
import nltk
from wordcloud import WordCloud
from nltk.corpus import stopwords
from pyspark import SparkContext
from os import path
import matplotlib.pyplot as plt
def charRemoveFromString(x):
	cleanString = ''.join(filter(str.isalnum, x)) 
	return cleanString
		
# create Spark context with necessary configuration
sc = SparkContext("local","PySpark Word Count Exmaple")
stopWords=stopwords.words('english')
stopWords.append('')
# read data from text file and split each line into words and removing stop words
nyt = sc.textFile("/Users/venkatavarunnelakuditi/Desktop/input.txt").flatMap(lambda line: line.split(" ")).map(lambda x: x.strip()).map(lambda x: charRemoveFromString(x)).map(lambda x:x.lower()).filter(lambda x: x not in stopWords)
    
# count the occpythurrence of each word
wordCounts = nyt.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b).map(lambda a:[a[1],a[0]]).sortByKey(False)

wordCounts.saveAsTextFile("/Users/venkatavarunnelakuditi/Desktop/spark")

#making a list of top 100 words
wordCounts=wordCounts.take(100)
#print(wordCounts)


wordFrequency=dict()
for x in wordCounts:
	wordFrequency[x[1]]=x[0]
#print(wordFrequency)
#generating word cloud
wc = WordCloud().generate_from_frequencies(dict(wordFrequency))

plt.imshow(wc, interpolation='bilinear')
plt.axis("off")
plt.show()
