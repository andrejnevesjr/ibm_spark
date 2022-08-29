# pip install -Uq "skillsnetwork>=0.20.6"
from icecream import ic
import pyspark
from pathlib import Path
import skillsnetwork
from packaging import version
import findspark
findspark.init()


def print_path():
    for path in Path("LabData").iterdir():
        print(path)


def get_spark_context():
    sc = pyspark.SparkContext.getOrCreate()
    return sc


def spark_job(data_file):
    # filter out the lines that contains INFO
    output = data_file.filter(lambda lines: "INFO" in lines)
    # Count the lines
    ic(output.count())
    # Count the lines with "spark" in it by combining transformation and action
    ic(output.filter(lambda lines: "spark" in lines).count())
    # Fetch those lines as an array of Strings
    output.filter(lambda lines: "spark" in lines).collect()
    # View the graph of an RDD
    ic(output.toDebugString())


def main():

    if not hasattr(skillsnetwork, "__version__") or version.parse(skillsnetwork.__version__) < version.parse("0.20.6"):
        raise ValueError(
            "Please install skillsnetwork>=0.20.6 or this lab won't work.")

    spark = get_spark_context()
    logFile = spark.textFile("LabData/notebook.log")
    spark_job(logFile)


if __name__ == '__main__':
    main()
