import sys
from stocktick import StockTick
from pyspark import SparkContext
from pyspark import SparkConf
#from pyspark import StorageLevel


def maxValuesReduce(a, b):
	###  Return a StockTick object with the maximum value between a and b for each one of its 
	###  four fields (price, bid, ask, units)
	maxTick = StockTick(None, "", "", max(a.price,b.price), max(a.bid,b.bid), max(a.ask,b.ask), max(a.units,b.units))
	return maxTick

def minValuesReduce(a, b):
	###  Return a StockTick object with the minimum value between a and b for each one of its
	###  four fields (price, bid, ask, units)
	minTick = StockTick(None, "", "", min(a.price,b.price), min(a.bid,b.bid), min(a.ask,b.ask), min(a.units,b.units))
	return minTick

def generateSpreadsHourlyKeys(tick):  
	date = tick.date.split("/")
	time = tick.time.split(":")
	key = date[2]+"-"+date[0]+"-"+date[1]+":"+time[0]
	spread = float(tick.ask-tick.bid)/(2*(tick.ask+tick.bid))
	return (key,(tick.time,spread,1))
	
def generateSpreadsDailyKeys(tick): 
	date = tick.date.split("/")
	key = date[2]+"-"+date[0]+"-"+date[1]
	spread = float(tick.ask-tick.bid)/(2*(tick.ask+tick.bid))
	return (key,(tick.time,spread,1))

def spreadsSumReduce(a, b):          
	if a[0] != b[0]:
		return (b[0],a[1]+b[1],a[2]+b[2])
	else:
		return a
	


if __name__ == "__main__":
	"""
	Usage: stock
	"""
	conf = SparkConf().setAppName("StockTick").set("spark.executor.memory", "1g")
	sc = SparkContext(conf=conf)

	# rawTickData is a Resilient Distributed Dataset (RDD)
	#rawTickData = sc.textFile("WDC_tickbidask_short.txt")
	rawTickData = sc.textFile("/cs/bigdata/datasets/WDC_tickbidask.txt") 
			
	tickData =  rawTickData.map(lambda line : StockTick(line))
	###  use map to convert each line into a StockTick object

	goodTicks = tickData.filter(lambda tick : tick.price > 0 and tick.bid > 0 and tick.ask > 0 and tick.units > 0)
	### use filter to only keep records for which all fields are > 0

	#goodTicks.persist(StorageLevel.MEMORY_ONLY) ###  store goodTicks in the in-memory cache
	goodTicks.cache()

	#numTicks =  goodTicks.count() ###  count the number of lines in this RDD


	#sumValues = goodTicks.reduce(lambda a,b: StockTick(None,"","",a.price+b.price,a.bid+b.bid,a.ask+b.ask,a.units+b.units))
	###  use goodTicks.reduce(lambda a,b: StockTick(???)) to sum the price, bid, 
		      ### ask and unit fields
	#maxValuesReduce = goodTicks.reduce(maxValuesReduce) ### write the maxValuesReduce function
	#minValuesReduce = goodTicks.reduce(minValuesReduce) ### write the minValuesReduce function

	#avgUnits = sumValues.units / float(numTicks)
	#avgPrice = sumValues.price / float(numTicks)
	#print "Max units %i, avg units %f\n" % (maxValuesReduce.units, avgUnits)
	#print "Max price %f, min price %f, avg price %f\n" % (maxValuesReduce.price, minValuesReduce.price, avgPrice)

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

	avgDailySpreads = goodTicks.map(generateSpreadsDailyKeys).reduceByKey(spreadsSumReduce)   # (1)
	avgDailySpreads = avgDailySpreads.map(lambda (key,a): (key,a[1]/a[2]))     					# (2)
	avgDailySpreads = avgDailySpreads.sortByKey().map(lambda (key,a): key+","+str(a))         # (3)
	avgDailySpreads = avgDailySpreads.saveAsTextFile("WDC_daily")                             # (4)

	# For the monthly spread you only need to change the key. How?
	avgDailySpreads = goodTicks.map(generateSpreadsHourlyKeys).reduceByKey(spreadsSumReduce)  # (1)
	avgDailySpreads = avgDailySpreads.map(lambda (key,a): (key,a[1]/a[2]))                    # (2)
	avgDailySpreads = avgDailySpreads.sortByKey().map(lambda (key,a): key+","+str(a))			# (3)
	avgDailySpreads = avgDailySpreads.saveAsTextFile("WDC_hourly")                            # (4)

	sc.stop()

