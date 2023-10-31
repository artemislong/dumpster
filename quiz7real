import sys
import datetime
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    # Setup
    sc = SparkContext(appName="PythonStreamingDemo")
    ssc = StreamingContext(sc, 1)

    # Create the stream of port 9999 on localhost
    numbers_stream = ssc.socketTextStream("localhost", 9999)

    # Create some streams based on the previous stream
    # last_5_numbers = numbers_stream.window(5, 1) # Last five seconds of numbers, every second
    # max_of_last_5_numbers = last_5_numbers.map(lambda x: int(x)).reduce(lambda x, y: max(x, y)).map(lambda m: "Max: " + str(m))
    # even_numbers_of_last_5_numbers = last_5_numbers.filter(lambda x: int(x) % 2 == 0)

    # Printing streams 
    numbers_stream.pprint()
    # last_5_numbers.pprint()
    # max_of_last_5_numbers.pprint()
    # even_numbers.pprint()

    ssc.start()
    ssc.awaitTermination()
