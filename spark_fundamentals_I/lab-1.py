# pip install -Uq "skillsnetwork>=0.20.6"
import pyspark
import asyncio
import os
from pathlib import Path
import skillsnetwork
from packaging import version
import findspark
findspark.init()


async def prepare_data():
    # Download the data from the IBM server.
    # # This may take ~30 seconds depending on your internet speed.
    await skillsnetwork.prepare("https://cocl.us/BD0211EN_Data", overwrite=True)


def print_path():
    for path in Path("LabData").iterdir():
        print(path)


def get_spark_context():
    sc = pyspark.SparkContext.getOrCreate()
    return sc


def main():

    if not hasattr(skillsnetwork, "__version__") or version.parse(skillsnetwork.__version__) < version.parse("0.20.6"):
        raise ValueError(
            "Please install skillsnetwork>=0.20.6 or this lab won't work.")

    if not os.listdir("./LabData"):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(prepare_data())
        loop.close()
        print_path()

    # Get Spark Context
    spark = get_spark_context()
    # Get Spark Version
    spark.version
    # Add in the path to the README.md file in LabData.
    readme = spark.textFile("LabData/README.md")
    # Let’s perform some RDD actions on this text file. Count the number of items in the RDD using this command:
    readme.count()
    # Let’s run another action. Run this command to find the first item in the RDD:
    readme.first()
    # Use the filter transformation to return a new RDD with a subset of the items in the file.
    linesWithSpark = readme.filter(lambda line: "Spark" in line)
    # You can even chain together transformations and actions. To find out how many lines contains the word “Spark”
    readme.filter(lambda line: "Spark" in line).count()
    # find the line from that "README.md" file with the most words in it.
    most_words_on_it = (readme
                        .map(lambda line: len(line.split()))
                        .reduce(lambda a, b: a if (a > b) else b))

    # Auxiliar function - use any top level Python functions.
    def my_max(a, b):
        if a > b:
            return a
        else:
            return b

    readme.map(lambda line: len(line.split())).reduce(my_max)

    # MAP REDUCE DATA FLOW PATTERN #
    # Spark has a MapReduce data flow pattern. We can use this to do a word count on the readme file.
    wordCounts = (readme
                  .flatMap(lambda line: line.split())
                  .map(lambda word: (word, 1))
                  .reduceByKey(lambda a, b: a+b))
    # It is recommended to use collect() for testing only
    wordCounts.collect()
    # determine what is the most frequent word in the README. Answer  => ('the', 21)
    wordCounts.reduce(lambda a, b: a if (a[1] > b[1]) else b)

    # SPARK CACHING #
    # Once you run the second count() operation, you should notice a small increase in speed
    print(linesWithSpark.count())

    from timeit import Timer

    def count():
        return linesWithSpark.count()

    t = Timer(lambda: count())
    print(t.timeit(number=50))
    linesWithSpark.cache()
    print(t.timeit(number=50))


if __name__ == '__main__':
    main()
