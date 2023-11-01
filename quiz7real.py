import sys
import datetime
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    # Function to collect DStream data into a list
    def collect_to_list(rdd_list):
        # Convert each RDD in the list to a list
        collected_data = [rdd.collect() for rdd in rdd_list]
        return collected_data
    
    # Setup
    sc = SparkContext(appName="PythonStreamingDemo")
    ssc = StreamingContext(sc, 1)

    # Create the stream of port 9999 on localhost
    main_stream = ssc.socketTextStream("localhost", 9999)

    # Q1
    split = main_stream.flatMap(lambda line: line.split(" "))
    # Create DStreams to filter data for Google and Microsoft stocks
    date = main_stream.map(lambda line: line.split(" ")[0])
    googPrice = main_stream.map(lambda line: float(line.split(" ")[1]))
    msftPrice = main_stream.map(lambda line: float(line.split(" ")[2]))
    main_stream.map(lambda line: "Date: " + line.split(" ")[0] + ", GOOGLE: " + line.split(" ")[1] + ", MICROSOFT: " + line.split(" ")[2]).pprint()
    
    # Q2
    # get last days
    # Calculate moving averages and filter out windows with insufficient data
    goog10Day = googPrice.window(10, 1).map(lambda x: (x,1)).reduce(lambda x,y: (x[0]+y[0], x[1]+y[1])).filter(lambda xy: xy[1] == 10).map(lambda xy: xy[0] / xy[1])
    goog40Day = googPrice.window(40, 1).map(lambda x: (x,1)).reduce(lambda x,y: (x[0]+y[0], x[1]+y[1])).filter(lambda xy: xy[1] == 40).map(lambda xy: xy[0] / xy[1])
    msft10Day = msftPrice.window(10, 1).map(lambda x: (x,1)).reduce(lambda x,y: (x[0]+y[0], x[1]+y[1])).filter(lambda xy: xy[1] == 10).map(lambda xy: xy[0] / xy[1])
    msft40Day = msftPrice.window(40, 1).map(lambda x: (x,1)).reduce(lambda x,y: (x[0]+y[0], x[1]+y[1])).filter(lambda xy: xy[1] == 40).map(lambda xy: xy[0] / xy[1])
    # Print the results (or do further processing)
    goog10Day.map(lambda x: "Avg GOOG 10days: " + str(x)).pprint()
    goog40Day.map(lambda x: "Avg GOOG 40days: " + str(x)).pprint()
    msft10Day.map(lambda x: "Avg MSFT 10days: " + str(x)).pprint()
    msft40Day.map(lambda x: "Avg MSFT 40days: " + str(x)).pprint()

    
    ssc.start()
    ssc.awaitTermination()
