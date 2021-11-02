from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pb2df import message_to_row, schema_for
from pb.example_pb2 import ComplexMessage


@F.udf(returnType=schema_for(ComplexMessage().DESCRIPTOR))
def specific_message_bytes_to_row(pb_bytes):
    msg = ComplexMessage.FromString(pb_bytes)
    row = message_to_row(ComplexMessage().DESCRIPTOR, msg)
    return row


if __name__ == "__main__":

    spark_conf = SparkConf().setMaster("local").set("spark.driver.memory", "2G")

    sc = SparkContext(conf=spark_conf)
    sc.setLogLevel("ERROR")

    spark = SparkSession(sparkContext=sc)

    # kafka source
    sdf = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "topic")
        .load()
    )

    sdf = sdf.withColumn("msg", specific_message_bytes_to_row("value")).select("msg")

    print("kafka source schema:")
    sdf.printSchema()

    query = (
        sdf.writeStream.outputMode("update")
        .format("console")
        .option("truncate", "false")
        .start()
    )
    query.awaitTermination()

    spark.stop()
