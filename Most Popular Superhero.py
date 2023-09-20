from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

def parser(line):
    fields = line.strip().split()
    count=len(fields)
    return int(fields[0]),count-1


spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("paste your Marvel+Names.txt path here")
lines = spark.sparkContext.textFile("paste your Marvel+graph.txt path here")

parsedLines=lines.map(parser)

schema1 = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("count", IntegerType(), True)])

parsedLinesdf = spark.createDataFrame(parsedLines, schema1)

parsedLinesdf.createOrReplaceTempView("super")

sorteddf=spark.sql("select id,sum(count) as sumsh from super group by id order by sumsh desc")

# parsedLinesSorted=parsedLinesdf.sort("count",ascending=False)

# sorteddf.show()

sorteddf.createOrReplaceTempView("sorteddf")
names.createOrReplaceTempView("names")

superhero=spark.sql("select name as Superhero,sumsh as Count from names join sorteddf on names.id=sorteddf.id order by Count desc")
superhero.show()