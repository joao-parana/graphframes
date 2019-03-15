# Graphframes Examples

## On-Time Flight Performance with GraphFrames for Apache Spark

```bash
cat DATA/tripVertices.csv 

id,City,State,Country
STL,St. Louis,MO,USA
EKO,Elko,NV,USA
RAP,Rapid City,SD,USA
GRK,Killeen,TX,USA
MSY,New Orleans,LA,USA
LGA,New York,NY,USA
JFK,New York,NY,USA
EWN,New Bern,NC,USA
. . .
```


```bash
DATA/tripEdges.csv 
tripid,delay,src,dst,city_dst,state_dst
1011158,-5,SAN,IAH,Houston,TX
1011516,-8,SAN,IAH,Houston,TX
1010937,16,SAN,IAH,Houston,TX
1010702,1,SAN,IAH,Houston,TX
1010620,3,SAN,IAH,Houston,TX
```

Note: `src` and `dst` from `tripEdges` are `id` on `tripVertices`


```python
# Build `tripGraph` GraphFrame
#  This GraphFrame builds up on the vertices and edges based on our trips (flights)
tripGraph = GraphFrame(tripVertices, tripEdges)
print tripGraph

# Build `tripGraphPrime` GraphFrame
#   This graphframe contains a smaller subset of data to make it easier to display motifs and subgraphs (below)
tripEdgesPrime = departureDelays_geo.select("tripid", "delay", "src", "dst")
tripGraphPrime = GraphFrame(tripVertices, tripEdgesPrime)

# Let's start with a set of simple graph queries to understand flight performance and departure delays
#Determine the number of airports and trips 
print "Airports: %d" % tripGraph.vertices.count()
print "Trips: %d" % tripGraph.edges.count()

# Determining the longest delay in this dataset
longestDelay = tripGraph.edges.groupBy().max("delay")
display(longestDelay)

# Determining number of on-time / early flights vs. delayed flights
print "On-time / Early Flights: %d" % tripGraph.edges.filter("delay <= 0").count()
print "Delayed Flights: %d" % tripGraph.edges.filter("delay > 0").count()

# What flights departing SFO are most likely to have significant delays
# Note, delay can be <= 0 meaning the flight left on time or early

sfoDelay = tripGraph.edges\
  .filter("src = 'SFO' and delay > 0")\
  .groupBy("src", "dst")\
  .avg("delay")\
  .sort(desc("avg(delay)"))

display(sfoDelay)  

# What destinations tend to have delays

# After displaying tripDelays, use Plot Options to set `state_dst` as a Key.
tripDelays = tripGraph.edges.filter("delay > 0")
display(tripDelays)

```

![USA delays](usa-airport-delays.png)

```python
# What destinations tend to have significant delays departing from SEA

# States with the longest cumulative delays (with individual delays > 100 minutes) (origin: Seattle)
display(tripGraph.edges.filter("src = 'SEA' and delay > 100"))

```

![SEA delays](sea-airport-delays.png)


```python
# Vertex Degrees
# inDegrees: Incoming connections to the airport
# outDegrees: Outgoing connections from the airport
# degrees: Total connections to and from the airport
# Reviewing the various properties of the property graph to understand the incoming and outgoing connections between airports.
# 
# Degrees
#  The number of degrees - the number of incoming and outgoing connections - for various airports within this sample dataset
display(tripGraph.degrees.sort(desc("degree")).limit(20))

# City / Flight Relationships through Motif Finding
# To more easily understand the complex relationship of city airports and their flights with each other, we can use motifs to find patterns of airports (i.e. vertices) connected by flights (i.e. edges). The result is a DataFrame in which the column names are given by the motif keys.
# What delays might we blame on SFO
# 
# Using tripGraphPrime to more easily display 
#   - The associated edge (ab, bc) relationships 
#   - With the different the city / airports (a, b, c) where SFO is the connecting city (b)
#   - Ensuring that flight ab (i.e. the flight to SFO) occured before flight bc (i.e. flight leaving SFO)
#   - Note, TripID was generated based on time in the format of MMDDHHMM converted to int
#       - Therefore bc.tripid < ab.tripid + 10000 means the second flight (bc) occured within approx a day of the first flight (ab)
# Note: In reality, we would need to be more careful to link trips ab and bc.
motifs = tripGraphPrime.find("(a)-[ab]->(b); (b)-[bc]->(c)")\
  .filter("(b.id = 'SFO') and (ab.delay > 500 or bc.delay > 500) and bc.tripid > ab.tripid and bc.tripid < ab.tripid + 10000")
display(motifs)

# Determining Airport Ranking using PageRank
# There are a large number of flights and connections through these various airports included in this Departure Delay Dataset. Using the pageRank algorithm, Spark iteratively traverses the graph and determines a rough estimate of how important the airport is.
#
# Determining Airport ranking of importance using `pageRank`
ranks = tripGraph.pageRank(resetProbability=0.15, maxIter=5)
display(ranks.vertices.orderBy(ranks.vertices.pagerank.desc()).limit(20))

# Most popular flights (single city hops)
# Using the tripGraph, we can quickly determine what are the most popular single city hop flights
# 
# Determine the most popular flights (single city hops)
import pyspark.sql.functions as func
topTrips = tripGraph \
  .edges \
  .groupBy("src", "dst") \
  .agg(func.count("delay").alias("trips")) 

# Show the top 5 most popular flights (single city hops)
display(topTrips.orderBy(topTrips.trips.desc()).limit(5))

```

![top Five Trips](topFiveTrips.png)


```python
# Top Transfer Cities
# Many airports are used as transfer points instead of the final Destination. An easy way to calculate this is by calculating the ratio of inDegree (the number of flights to the airport) / outDegree (the number of flights leaving the airport). Values close to 1 may indicate many transfers, whereas values < 1 indicate many outgoing flights and > 1 indicate many incoming flights. Note, this is a simple calculation that does not take into account of timing or scheduling of flights, just the overall aggregate number within the dataset.
# 
# Calculate the inDeg (flights into the airport) and outDeg (flights leaving the airport)
inDeg = tripGraph.inDegrees
outDeg = tripGraph.outDegrees
​
# Calculate the degreeRatio (inDeg/outDeg)
degreeRatio = inDeg.join(outDeg, inDeg.id == outDeg.id) \
  .drop(outDeg.id) \
  .selectExpr("id", "double(inDegree)/double(outDegree) as degreeRatio") \
  .cache()
​
# Join back to the `airports` DataFrame (instead of registering temp table as above)
nonTransferAirports = degreeRatio.join(airports, degreeRatio.id == airports.IATA) \
  .selectExpr("id", "city", "degreeRatio") \
  .filter("degreeRatio < .9 or degreeRatio > 1.1")
​
# List out the city airports which have abnormal degree ratios.
display(nonTransferAirports)

```


```csv
id,city,degreeRatio
BRW,Barrow,0.28651685393258425
GFK,Grand Forks,1.3333333333333333
FAI,Fairbanks,1.1232686980609419
OME,Nome,0.5084745762711864

```

```python
# Join back to the `airports` DataFrame (instead of registering temp table as above)
transferAirports = degreeRatio.join(airports, degreeRatio.id == airports.IATA) \
  .selectExpr("id", "city", "degreeRatio") \
  .filter("degreeRatio between 0.9 and 1.1")
  
# List out the top 10 transfer city airports
display(transferAirports.orderBy("degreeRatio").limit(10))
```


```python
# Breadth First Search
# Breadth-first search (BFS) is designed to traverse the graph to quickly find the desired vertices (i.e. airports) and edges (i.e flights). Let's try to find the shortest number of connections between cities based on the dataset. Note, these examples do not take into account of time or distance, just hops between cities.
# 
# Example 1: Direct Seattle to San Francisco 
filteredPaths = tripGraph.bfs(
  fromExpr = "id = 'SEA'",
  toExpr = "id = 'SFO'",
  maxPathLength = 1)
display(filteredPaths)

```


```csv
from,e0,to
"{""id"":""SEA"",""City"":""Seattle"",""State"":""WA"",""Country"":""USA""}","{""tripid"":1010710,""delay"":31,""src"":""SEA"",""dst"":""SFO"",""city_dst"":""San Francisco"",""state_dst"":""CA""}","{""id"":""SFO"",""City"":""San Francisco"",""State"":""CA"",""Country"":""USA""}"
"{""id"":""SEA"",""City"":""Seattle"",""State"":""WA"",""Country"":""USA""}","{""tripid"":1012125,""delay"":-4,""src"":""SEA"",""dst"":""SFO"",""city_dst"":""San Francisco"",""state_dst"":""CA""}","{""id"":""SFO"",""City"":""San Francisco"",""State"":""CA"",""Country"":""USA""}"
"{""id"":""SEA"",""City"":""Seattle"",""State"":""WA"",""Country"":""USA""}","{""tripid"":1011840,""delay"":-5,""src"":""SEA"",""dst"":""SFO"",""city_dst"":""San Francisco"",""state_dst"":""CA""}","{""id"":""SFO"",""City"":""San Francisco"",""State"":""CA"",""Country"":""USA""}"
"{""id"":""SEA"",""City"":""Seattle"",""State"":""WA"",""Country"":""USA""}","{""tripid"":1010610,""delay"":-4,""src"":""SEA"",""dst"":""SFO"",""city_dst"":""San Francisco"",""state_dst"":""CA""}","{""id"":""SFO"",""City"":""San Francisco"",""State"":""CA"",""Country"":""USA""}"
"{""id"":""SEA"",""City"":""Seattle"",""State"":""WA"",""Country"":""USA""}","{""tripid"":1011230,""delay"":-2,""src"":""SEA"",""dst"":""SFO"",""city_dst"":""San Francisco"",""state_dst"":""CA""}","{""id"":""SFO"",""City"":""San Francisco"",""State"":""CA"",""Country"":""USA""}"
"{""id"":""SEA"",""City"":""Seattle"",""State"":""WA"",""Country"":""USA""}","{""tripid"":1010955,""delay"":-6,""src"":""SEA"",""dst"":""SFO"",""city_dst"":""San Francisco"",""state_dst"":""CA""}","{""id"":""SFO"",""City"":""San Francisco"",""State"":""CA"",""Country"":""USA""}"
"{""id"":""SEA"",""City"":""Seattle"",""State"":""WA"",""Country"":""USA""}","{""tripid"":1011100,""delay"":2,""src"":""SEA"",""dst"":""SFO"",""city_dst"":""San Francisco"",""state_dst"":""CA""}","{""id"":""SFO"",""City"":""San Francisco"",""State"":""CA"",""Country"":""USA""}"
"{""id"":""SEA"",""City"":""Seattle"",""State"":""WA"",""Country"":""USA""}","{""tripid"":1011405,""delay"":0,""src"":""SEA"",""dst"":""SFO"",""city_dst"":""San Francisco"",""state_dst"":""CA""}","{""id"":""SFO"",""City"":""San Francisco"",""State"":""CA"",""Country"":""USA""}"
"{""id"":""SEA"",""City"":""Seattle"",""State"":""WA"",""Country"":""USA""}","{""tripid"":1020710,""delay"":-1,""src"":""SEA"",""dst"":""SFO"",""city_dst"":""San Francisco"",""state_dst"":""CA""}","{""id"":""SFO"",""City"":""San Francisco"",""State"":""CA"",""Country"":""USA""}"
"{""id"":""SEA"",""City"":""Seattle"",""State"":""WA"",""Country"":""USA""}","{""tripid"":1022125,""delay"":-4,""src"":""SEA"",""dst"":""SFO"",""city_dst"":""San Francisco"",""state_dst"":""CA""}","{""id"":""SFO"",""City"":""San Francisco"",""State"":""CA"",""Country"":""USA""}"
"{""id"":""SEA"",""City"":""Seattle"",""State"":""WA"",""Country"":""USA""}","{""tripid"":1021840,""delay"":-5,""src"":""SEA"",""dst"":""SFO"",""city_dst"":""San Francisco"",""state_dst"":""CA""}","{""id"":""SFO"",""City"":""San Francisco"",""State"":""CA"",""Country"":""USA""}"
. . .
```


```python
# As you can see, there are a number of direct flights between Seattle and San Francisco.
# But there are no direct flights between San Francisco and Buffalo.
# But there are flights from San Francisco to Buffalo with Minneapolis as the transfer point.

# Example 2a: Direct San Francisco and Buffalo
filteredPaths = tripGraph.bfs(
  fromExpr = "id = 'SFO'",
  toExpr = "id = 'BUF'",
  maxPathLength = 1)
display(filteredPaths)
```

```python
# Example 2b: Flying from San Francisco to Buffalo with transfer point
filteredPaths = tripGraph.bfs(
  fromExpr = "id = 'SFO'",
  toExpr = "id = 'BUF'",
  maxPathLength = 2)
display(filteredPaths)

```


```csv
from,e0,v1,e1,to
"{""id"":""SFO"",""City"":""San Francisco"",""State"":""CA"",""Country"":""USA""}","{""tripid"":1010630,""delay"":-3,""src"":""SFO"",""dst"":""MSP"",""city_dst"":""Minneapolis"",""state_dst"":""MN""}","{""id"":""MSP"",""City"":""Minneapolis"",""State"":""MN"",""Country"":""USA""}","{""tripid"":2091325,""delay"":-5,""src"":""MSP"",""dst"":""BUF"",""city_dst"":""Buffalo"",""state_dst"":""NY""}","{""id"":""BUF"",""City"":""Buffalo"",""State"":""NY"",""Country"":""USA""}"
"{""id"":""SFO"",""City"":""San Francisco"",""State"":""CA"",""Country"":""USA""}","{""tripid"":1010630,""delay"":-3,""src"":""SFO"",""dst"":""MSP"",""city_dst"":""Minneapolis"",""state_dst"":""MN""}","{""id"":""MSP"",""City"":""Minneapolis"",""State"":""MN"",""Country"":""USA""}","{""tripid"":1051520,""delay"":37,""src"":""MSP"",""dst"":""BUF"",""city_dst"":""Buffalo"",""state_dst"":""NY""}","{""id"":""BUF"",""City"":""Buffalo"",""State"":""NY"",""Country"":""USA""}"
"{""id"":""SFO"",""City"":""San Francisco"",""State"":""CA"",""Country"":""USA""}","{""tripid"":1010830,""delay"":52,""src"":""SFO"",""dst"":""MSP"",""city_dst"":""Minneapolis"",""state_dst"":""MN""}","{""id"":""MSP"",""City"":""Minneapolis"",""State"":""MN"",""Country"":""USA""}","{""tripid"":2091325,""delay"":-5,""src"":""MSP"",""dst"":""BUF"",""city_dst"":""Buffalo"",""state_dst"":""NY""}","{""id"":""BUF"",""City"":""Buffalo"",""State"":""NY"",""Country"":""USA""}"
"{""id"":""SFO"",""City"":""San Francisco"",""State"":""CA"",""Country"":""USA""}","{""tripid"":1010830,""delay"":52,""src"":""SFO"",""dst"":""MSP"",""city_dst"":""Minneapolis"",""state_dst"":""MN""}","{""id"":""MSP"",""City"":""Minneapolis"",""State"":""MN"",""Country"":""USA""}","{""tripid"":1051520,""delay"":37,""src"":""MSP"",""dst"":""BUF"",""city_dst"":""Buffalo"",""state_dst"":""NY""}","{""id"":""BUF"",""City"":""Buffalo"",""State"":""NY"",""Country"":""USA""}"
"{""id"":""SFO"",""City"":""San Francisco"",""State"":""CA"",""Country"":""USA""}","{""tripid"":1011300,""delay"":-6,""src"":""SFO"",""dst"":""MSP"",""city_dst"":""Minneapolis"",""state_dst"":""MN""}","{""id"":""MSP"",""City"":""Minneapolis"",""State"":""MN"",""Country"":""USA""}","{""tripid"":2091325,""delay"":-5,""src"":""MSP"",""dst"":""BUF"",""city_dst"":""Buffalo"",""state_dst"":""NY""}","{""id"":""BUF"",""City"":""Buffalo"",""State"":""NY"",""Country"":""USA""}"
"{""id"":""SFO"",""City"":""San Francisco"",""State"":""CA"",""Country"":""USA""}","{""tripid"":1011300,""delay"":-6,""src"":""SFO"",""dst"":""MSP"",""city_dst"":""Minneapolis"",""state_dst"":""MN""}","{""id"":""MSP"",""City"":""Minneapolis"",""State"":""MN"",""Country"":""USA""}","{""tripid"":1051520,""delay"":37,""src"":""MSP"",""dst"":""BUF"",""city_dst"":""Buffalo"",""state_dst"":""NY""}","{""id"":""BUF"",""City"":""Buffalo"",""State"":""NY"",""Country"":""USA""}"
"{""id"":""SFO"",""City"":""San Francisco"",""State"":""CA"",""Country"":""USA""}","{""tripid"":1020630,""delay"":-7,""src"":""SFO"",""dst"":""MSP"",""city_dst"":""Minneapolis"",""state_dst"":""MN""}","{""id"":""MSP"",""City"":""Minneapolis"",""State"":""MN"",""Country"":""USA""}","{""tripid"":2091325,""delay"":-5,""src"":""MSP"",""dst"":""BUF"",""city_dst"":""Buffalo"",""state_dst"":""NY""}","{""id"":""BUF"",""City"":""Buffalo"",""State"":""NY"",""Country"":""USA""}"
. . .

```

### Visualizing Flights Using D3

To get a powerful visualization of the flight paths and connections in this dataset, we can leverage the Airports D3 visualization within our Databricks notebook.  By connecting our GraphFrames, DataFrames, and D3 visualizations, we can visualize the scope of all of the flight connections as noted below for all on-time or early departing flights within this dataset.  The blue circles represent the vertices (i.e. airports) where the size of the circle represents the number of edges (i.e. flights) in and out of those airports.  The black lines are the edges themselves (i.e. flights) and their respective connections to the other vertices (i.e. airports).  Note for any edges that go offscreen, they are representing vertices (i.e. airports) in the states of Hawaii and Alaska.


```scala
// Loading the D3 Visualization
// Using the airports D3 visualization to visualize airports and flight paths
// 
// %scala package d3a // We use a package object so that we can define top level cl ...
//
// %scala d3a.graphs.help()
//
// Visualize On-time and Early Arrivals
// Produces a force-directed graph given a collection of edges of the following form:
case class Edge(src: String, dest: String, count: Long)

// Usage:
// %scala
import d3._
graphs.force(
  height = 500,
  width = 500,
  clicks: Dataset[Edge])

// %scala
// On-time and Early Arrivals
import d3a._
graphs.force(
  height = 800,
  width = 1200,
  clicks = sql("""select src, dst as dest, count(1) as count 
  from departureDelays_geo 
  where delay <= 0 group by src, dst
  """).as[Edge])

```

![d3 Arrivals](d3Arrivals.png)

mOvie view: ![d3 Arrivals movie](https://databricks.com/wp-content/uploads/2016/03/airports-d3-m.gif)

```python

```

