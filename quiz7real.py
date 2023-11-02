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
    goog10Day = googPrice.window(5, 1).map(lambda x: (x[0],x[1],1)).reduce(lambda x,y: (max(x[0], y[0]), x[1]+y[1], x[2]+y[2])).filter(lambda xy: xy[2] == 5).map(lambda xy: (xy[0], str(xy[1] / xy[2])))
    goog40Day = googPrice.window(10, 1).map(lambda x: (x[0],x[1],1)).reduce(lambda x,y: (max(x[0], y[0]), x[1]+y[1], x[2]+y[2])).filter(lambda xy: xy[2] == 10).map(lambda xy: (xy[0], str(xy[1] / xy[2])))
    msft10Day = msftPrice.window(5, 1).map(lambda x: (x[0],x[1],1)).reduce(lambda x,y: (max(x[0], y[0]), x[1]+y[1], x[2]+y[2])).filter(lambda xy: xy[2] == 5).map(lambda xy: (xy[0], str(xy[1] / xy[2])))
    msft40Day = msftPrice.window(10, 1).map(lambda x: (x[0],x[1],1)).reduce(lambda x,y: (max(x[0], y[0]), x[1]+y[1], x[2]+y[2])).filter(lambda xy: xy[2] == 10).map(lambda xy: (xy[0], str(xy[1] / xy[2])))

    # goog10Day.map(lambda x: "Avg GOOG 10days: " + str(x)).pprint()
    # goog40Day.map(lambda x: "Avg GOOG 40days: " + str(x)).pprint()
    # msft10Day.map(lambda x: "Avg MSFT 10days: " + str(x)).pprint()
    # msft40Day.map(lambda x: "Avg MSFT 40days: " + str(x)).pprint()
    
    # Q3
    # different logic
    googAvg10DayLast2 = goog10Day.window(2, 1)
    googAvg40DayLast2 = goog40Day.window(2, 1)
    
    googAvg2 = googAvg10DayLast2.join(googAvg40DayLast2) # (date, (avg10, avg40))
    # googAvg2.pprint()
    def timeDiff(x,y): 
        xs = list(map(int, x.split("-")))
        print(xs)
        ys = list(map(int, y.split("-")))
        diff = date(xs[0],xs[1],xs[2]) - date(ys[0],ys[1],ys[2])
        return diff.days/abs(diff.days)
    # googAvg2.map(lambda x: x[1]).pprint()
    # schema: date, date order (negative means first row is older), difference between avg10 and 40 first row and second row
    googAvg2Reduced = googAvg2.reduce(lambda x,y: (max(x[0],y[0]), timeDiff(x[0],y[0]), float(x[1][0]) - float(x[1][1])))
    # googAvg2Reduced = googAvg2.reduce(lambda x,y: (max(x[0],y[0]), timeDiff(x[0],y[0]), float(x[1][0]) - float(x[1][1]), float(y[1][0]) - float(y[1][1])))

    googAvg2Reduced.pprint()
    # transforming so the first entry is newer
    # first map: sorting differences, if x[1] is positive, x[2] is today's avg10/40 difference | 2nd map: sell if today's difference is positive, and previous wasn't
    def decide(x):
        return 1 if (x[1] > 0 & x[2] < 0) else 2 if (x[1] < 0 & x[2] > 0) else 0
    # googAvgDecision = googAvg2Reduced.map(lambda x: (x[0], x[2] if int(x[1]) > 0 else x[3], x[3] if int(x[1]) > 0 else x[2]))
    # googAvgDecision.pprint()
    # .map(lambda x: (x[0], decide(x)))
    # googAvg2Reduced.map(lambda x: str(x[0]) + " buy " if x[1] == 1 else " sell " if x[1] == 2 else " wait " + "GOOG").pprint()                                      
    # msftAvg10DayLast2 = msft10Day.window(2, 1)
    # msftAvg40DayLast2 = msft40Day.window(2, 1)
    # msftAvg2 = msftAvg10DayLast2.join(msftAvg40DayLast2)
    # msftAvg2.pprint()

    
    ssc.start()
    ssc.awaitTermination()
