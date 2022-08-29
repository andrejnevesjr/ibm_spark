# pip install -Uq "skillsnetwork>=0.20.6"
from icecream import ic
import pyspark
from pathlib import Path
import skillsnetwork
from packaging import version
import findspark
findspark.init()

# Auxiliar variable
accum = 0


def print_path():
    for path in Path("LabData").iterdir():
        print(path)


def get_spark_context():
    sc = pyspark.SparkContext.getOrCreate()
    return sc


def spark_job(file_a, file_b) -> None:
    # How many Spark keywords are in each file?
    file_a.filter(lambda line: "Spark" in line).count()
    file_b.filter(lambda line: "Spark" in line).count()

    # Now do a WordCount on each RDD so that the results are (K,V) pairs of (word,count)
    readmeCount = (file_a
                   .flatMap(lambda line: line.split("   "))
                   .map(lambda word: (word, 1))
                   .reduceByKey(lambda a, b: a+b)
                   )
    pomCount = (file_b
                .flatMap(lambda line: line.split("   "))
                .map(lambda word: (word, 1))
                .reduceByKey(lambda a, b: a+b)
                )

    # print("Readme Count\n")
    # print(readmeCount.collect())
    # print("Pom Count\n")
    # print(pomCount.collect())

    # The join function combines the two datasets (K,V) and (K,W) together and get (K, (V,W)). Let's join these two counts together.
    joined = readmeCount.join(pomCount)
    ic(joined.collect())
    # Let's combine the values together to get the total count
    joinedSum = joined.map(lambda k: (k[0], (k[1][0]+k[1][1])))
    # To check if it is correct, print the first five elements from the joined and the joinedSum RDD
    ic("Joined Individial\n")
    ic(joined.take(5))
    ic("\n\nJoined Sum\n")
    ic(joinedSum.take(5))


def main():

    global accum
    if not hasattr(skillsnetwork, "__version__") or version.parse(skillsnetwork.__version__) < version.parse("0.20.6"):
        raise ValueError(
            "Please install skillsnetwork>=0.20.6 or this lab won't work.")

    spark = get_spark_context()
    readMeFile = spark.textFile("LabData/README.md")
    pomFile = spark.textFile("LabData/pom.xml")

    # spark_job(readMeFile, pomFile)

    #  SHARED VARIABLES
    # broadcast
    broadcastVar = spark.broadcast([1, 2, 3])
    ic(broadcastVar.value)
    # accumulators
    # only driver reads the accumulator workers can only increment it
    accum = spark.accumulator(0)
    rdd = spark.parallelize([1, 2, 3, 4])

    def f(x):
        global accum
        accum += x

    rdd.foreach(f)
    ic(accum.value)

    #  KEY VALUES
    pair = ('a', 'b')
    ic(pair[0])
    ic(pair[1])


if __name__ == '__main__':
    main()
