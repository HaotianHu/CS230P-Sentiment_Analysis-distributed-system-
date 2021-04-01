from pyspark.sql import SparkSession
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
analyser = SentimentIntensityAnalyzer()

def calculate_sentiment_analyser(tokenized_text):
    return analyser.polarity_scores(tokenized_text)

def sentiment_calculator(body):
    score = 0
    sentiment_analyzer = calculate_sentiment_analyser(body)
    compound_score = sentiment_analyzer['compound']
    if compound_score <= -0.05:
        score = -1
    elif compound_score >= 0.05:
        score = 1
    else:
        score = 0
    return score


spark = SparkSession.builder.appName("read_csv").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('ERROR')

df = spark.read.csv("s3://cs230-proj//RedditComments_test.csv",header=True)
#df = spark.read.csv("in/RedditComments_test.csv",header=True)
rdd = df.rdd
rdd_1 = rdd.map(lambda x: (x['topic'], sentiment_calculator(x['body'])))\
    .combineByKey(lambda x: (x, 1),
                  lambda x, y: (x[0] + y, x[1] + 1),
                  lambda x, y: (x[0] + y[0], x[1] + y[1])).map(lambda x: (x[0], x[1][0] / x[1][1]))
df_after = rdd_1.toDF()
df_after.show()