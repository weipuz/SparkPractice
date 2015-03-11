import sys
from stocktick import StockTick
from pyspark import SparkContext
#from pyspark import StorageLevel


def maxValuesReduce(a, b):
   ### TODO: Return a StockTick object with the maximum value between a and b for each one of its 
   ### four fields (price, bid, ask, units)
   maxTick = StockTick(None, "", "", max(a.price,b.price), max(a.bid,b.bid), max(a.ask,b.ask), max(a.units,b.units))
   return maxTick

def minValuesReduce(a, b):
   ### TODO: Return a StockTick object with the minimum value between a and b for each one of its
   ### four fields (price, bid, ask, units)
   minTick = StockTick(None, "", "", min(a.price,b.price), min(a.bid,b.bid), min(a.ask,b.ask), min(a.units,b.units))
   return minTick

def generateSpreadsDailyKeys(tick):  
	### TODO: Write Me (see below)
	date = tick.date.split("/")
	key = date[2]+"-"+date[0]+"-"+date[1]
def generateSpreadsHourlyKeys(tick): ### TODO: Write Me (see below)
def spreadsSumReduce(a, b):          ### TODO: Write Me (see below)


if __name__ == "__main__":
   """
   Usage: stock
   """
   sc = SparkContext(appName="StockTick")

   # rawTickData is a Resilient Distributed Dataset (RDD)
   rawTickData = sc.textFile("WDC_tickbidask_short.txt") 
						
   tickData =  rawTickData.map(lambda line : StockTick(line))
   ### TODO: use map to convert each line into a StockTick object
   
   goodTicks = tickData.filter(lambda tick : tick.price > 0 and tick.bid > 0 and tick.ask > 0 and tick.units > 0)
   ### TODO: use filter to only keep records for which all fields are > 0
   
   #goodTicks.persist(StorageLevel.MEMORY_ONLY) ### TODO: store goodTicks in the in-memory cache
   goodTicks.cache()

   numTicks =  goodTicks.count() ### TODO: count the number of lines in this RDD
   

   sumValues = goodTicks.reduce(lambda a,b: StockTick(None,"","",a.price+b.price,a.bid+b.bid,a.ask+b.ask,a.units+b.units))
   ### TODO: use goodTicks.reduce(lambda a,b: StockTick(???)) to sum the price, bid, 
               ### ask and unit fields
   maxValuesReduce = goodTicks.reduce(maxValuesReduce) ### TODO: write the maxValuesReduce function
   minValuesReduce = goodTicks.reduce(minValuesReduce) ### TODO: write the minValuesReduce function

   avgUnits = sumValues.units / float(numTicks)
   avgPrice = sumValues.price / float(numTicks)
   print "Max units %i, avg units %f\n" % (maxValuesReduce.units, avgUnits)
   print "Max price %f, min price %f, avg price %f\n" % (maxValuesReduce.price, minValuesReduce.price, avgPrice)

   ### Daily and monthly spreads
   # Here is how the daily spread is computed. For each data point, the spread can be calculated 
   # using the following formulat : (avg - bid) / 2 * (avg + bid)
   # 1) We have a MapReduce phase that uses the generateSpreadsDailyKeys() function as an argument
   #    to map(), and the spreadsSumReduce() function as an argument to reduce()
   #    - The keys will be a unique date in the ISO 8601 format (so that sorting dates
   #      alphabetically will sort them chronologically)
   #    - The values will be tuples that contain adequates values to (1) only take one value into 
   #      account per tick (which value is picked doesn't matter), (2) sum the spreads for the 
   #      day, and (3) count the number of spread values that have been added.
   # 2) We have a Map phase that computes thee average spread using (b) and (c)
   # 3) A final Map phase formats the output by producing a string with the following format: 
   #    "<key (date)>, <average_spread>"
   # 4) The output is written using .saveAsTextFile("WDC_daily")

   avgDailySpreads = goodTicks.map(generateSpreadsDailyKeys).reduceByKey(spreadsSumReduce);  # (1)
   avgDailySpreads = avgDailySpreads.map(lambda a: ???)                                     # (2)
   avgDailySpreads = avgDailySpreads.sortByKey().map(lambda a: ???)                         # (3)
   avgDailySpreads = avgDailySpreads.saveAsTextFile("WDC_daily")                             # (4)


   # For the monthly spread you only need to change the key. How?

   #avgDailySpreads = goodTicks.map(generateSpreadsHourlyKeys).reduceByKey(spreadsSumReduce); # (1)
   #avgDailySpreads = avgDailySpreads.map(lambda a: ???)                                     # (2)
   #avgDailySpreads = avgDailySpreads.sortByKey().map(lambda a: ???)                         # (3)
   #avgDailySpreads = avgDailySpreads.saveAsTextFile("WDC_hourly")                            # (4)

   sc.stop()

