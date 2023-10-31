import sys
import datetime
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    # Setup
    sc = SparkContext(appName="PythonStreamingDemo")
    ssc = StreamingContext(sc, 1)

    # Create the stream of port 9999 on localhost
    main_stream = ssc.socketTextStream("localhost", 9999)

    # Q1
    split = main_stream.flatMap(lambda line: line.split(" "))
    # Create DStreams to filter data for Google and Microsoft stocks
    date = main_stream.map(lambda line: line.split(" ")[0])
    googPrice = main_stream.map(lambda line: (line.split(" ")[0], line.split(" ")[1]))
    msftPrice = main_stream.map(lambda line: (line.split(" ")[0], line.split(" ")[2]))
    
    # Q2
    date_count = date.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    # get counting number of entries for last 10 and 40 days
    # Sort the RDD by the date (key) in descending order
    sorted_rdd = date_count.transform(lambda rdd: rdd.sortByKey(ascending=False))
    # counting number of lines of last 40 days
    last_40_dates_counts = sorted_rdd.take(40)
    total_count40 = sum(count for date, count in last_40_dates_counts)
    # counting number of lines of last 10 days
    last_10_dates_counts = sorted_rdd.take(10)
    total_count10 = sum(count for date, count in last_10_dates_counts)

    # averaging last 10 days
    # AvgGoogPrice10 = 
    # averaging last 40 days
    

    # Printing streams 
    last_40_dates_counts.pprint()
    last_10_dates_counts.pprint()
    # last_5_numbers.pprint()
    # max_of_last_5_numbers.pprint()
    # even_numbers.pprint()

    ssc.start()
    ssc.awaitTermination()
