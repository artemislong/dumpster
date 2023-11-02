import sys
import datetime
from datetime import date
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":  
    # Setup
    sc = SparkContext(appName="PythonStreamingDemo")
    ssc = StreamingContext(sc, 1)
    
    # Create the stream of port 9999 on localhost
    main_stream = ssc.socketTextStream("localhost", 9999)

    # Q1
    split = main_stream.map(lambda line: line.split(" "))
    # Create DStreams to filter data for Google and Microsoft stocks
    googPrice = split.map(lambda line: (line[0], float(line[1])))
    msftPrice = split.map(lambda line: (line[0], float(line[2])))
    split.map(lambda line: "Date: " + line[0] + ", GOOGLE: " + line[1] + ", MICROSOFT: " + line[2]).pprint()
    
    # Q2
    # get last days
    # Calculate moving averages and filter out windows with insufficient data
    goog10Day = googPrice.window(10, 1).map(lambda x: (x[0],x[1],1)).reduce(lambda x,y: (max(x[0], y[0]), x[1]+y[1], x[2]+y[2])).filter(lambda xy: xy[2] == 10).map(lambda xy: (xy[0], str(xy[1] / xy[2])))
    goog40Day = googPrice.window(40, 1).map(lambda x: (x[0],x[1],1)).reduce(lambda x,y: (max(x[0], y[0]), x[1]+y[1], x[2]+y[2])).filter(lambda xy: xy[2] == 40).map(lambda xy: (xy[0], str(xy[1] / xy[2])))
    msft10Day = msftPrice.window(10, 1).map(lambda x: (x[0],x[1],1)).reduce(lambda x,y: (max(x[0], y[0]), x[1]+y[1], x[2]+y[2])).filter(lambda xy: xy[2] == 10).map(lambda xy: (xy[0], str(xy[1] / xy[2])))
    msft40Day = msftPrice.window(40, 1).map(lambda x: (x[0],x[1],1)).reduce(lambda x,y: (max(x[0], y[0]), x[1]+y[1], x[2]+y[2])).filter(lambda xy: xy[2] == 40).map(lambda xy: (xy[0], str(xy[1] / xy[2])))
    
    # Q3
    def timeDiff(x,y): 
        xs = list(map(int, x.split("-")))
        ys = list(map(int, y.split("-")))
        diff = date(xs[0],xs[1],xs[2]) - date(ys[0],ys[1],ys[2])
        return diff.days/abs(diff.days)
    # cross detection and advice: 1 is buy, 2 is sell, 0 is wait
    def decide(x):
        return 1 if ((x[1] > 0.0) & (x[2] < 0.0)) else 2 if ((x[1] < 0.0) & (x[2] > 0.0)) else 0

    # GOOG
    googAvg10DayLast2 = goog10Day.window(2, 1)
    googAvg40DayLast2 = goog40Day.window(2, 1)
    googAvg2 = googAvg10DayLast2.join(googAvg40DayLast2) # (date, (avg10, avg40))
    # schema: date, date order (negative means first row is older), difference between avg10 and 40 first row and second row
    googAvg2Reduced = googAvg2.map(lambda x: (x[0],x[1],1)).reduce(lambda x,y: (x[2]+y[2], max(x[0],y[0]), timeDiff(x[0],y[0]), float(x[1][0]) - float(x[1][1]), float(y[1][0]) - float(y[1][1]))).filter(lambda xy: xy[0] == 2)
    # sorting
    googAvgDecision = googAvg2Reduced.map(lambda x: (x[1], x[3] if int(x[2]) > 0 else x[4], x[4] if int(x[2]) < 0 else x[3]))
    googResult = googAvgDecision.map(lambda x: (x[0], decide(x)))
    goog = googResult.map(lambda x: (x[0], " buy " if x[1] == 1 else " sell " if x[1] == 2 else " wait ", "GOOG")).map(lambda x: x[0]+x[1]+x[2])
    goog.pprint()
    # MSFT 
    msftAvg10DayLast2 = msft10Day.window(2, 1)
    msftAvg40DayLast2 = msft40Day.window(2, 1)
    msftAvg2 = msftAvg10DayLast2.join(msftAvg40DayLast2) # (date, (avg10, avg40))
    # schema: date, date order (negative means first row is older), difference between avg10 and 40 first row and second row
    msftAvg2Reduced = msftAvg2.map(lambda x: (x[0],x[1],1)).reduce(lambda x,y: (x[2]+y[2], max(x[0],y[0]), timeDiff(x[0],y[0]), float(x[1][0]) - float(x[1][1]), float(y[1][0]) - float(y[1][1]))).filter(lambda xy: xy[0] == 2)
    # sorting
    msftAvgDecision = msftAvg2Reduced.map(lambda x: (x[1], x[3] if int(x[2]) > 0 else x[4], x[4] if int(x[2]) < 0 else x[3]))
    msftResult = msftAvgDecision.map(lambda x: (x[0], decide(x)))
    msft = msftResult.map(lambda x: (x[0], " buy " if x[1] == 1 else " sell " if x[1] == 2 else " wait ", "MSFT")).map(lambda x: x[0]+x[1]+x[2])
    msft.pprint()
    def write_to_text_file(rdd):
        with open("stock_decision.txt", "a") as file:  # Open the file in "append" mode
            for line in rdd.collect():
                file.write(line + "\n")
    goog.foreachRDD(write_to_text_file)
    msft.foreachRDD(write_to_text_file)
    
    ssc.start()
    ssc.awaitTermination()
