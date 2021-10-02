#libraries needed for this program
from pyspark import SparkContext
from nltk.corpus import stopwords
from collections import Counter

#function to remove charectors from words
def charRemoveFromString(x):
	cleanString = ''.join(filter(str.isalnum, x)) 
	return cleanString

#function to classify catogory Wise Top Words
def catogoryWiseTopWords(all_articals_In_Catagory):
    wordsList=list()
    flag=0
    for y in all_articals_In_Catagory:
        y=y.rsplit()
        y=[charRemoveFromString(word) for word in y if word not in stopwords.words('english')]
        wordsList.append(Counter(y))
    for y in wordsList:
        if(flag):
            tempcollector=y+tempcollector
        else:
            tempcollector=y
            flag=1
    return tempcollector.most_common(10)


# create Spark context with necessary configuration
sc = SparkContext("local","PySpark Word Count Exmaple")
stopWords=stopwords.words('english')
stopWords.append('')
# read data from text file and split each line into words and calling catogoryWiseTopWords
nytarticals=sc.wholeTextFiles("/Users/venkatavarunnelakuditi/Desktop/input.txt").flatMap(lambda lin: lin[1].split("URL: ")).filter(lambda x: x is not '').groupBy(lambda w: w[0:39]).map(lambda x : (x[0],catogoryWiseTopWords(list(x[1]))))
# read data from text file and split each line into words
nytarticals=nytarticals.collect()
for x in nytarticals:
    print(x)
