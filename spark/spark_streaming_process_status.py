from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pykafka import KafkaClient
import json
import sys
import pprint

def pushprocessStatusInKafka(status_counts):
    client = KafkaClient(hosts="ip-hostipaddress(type here):9092")
    topic = client.topics['process_ten_sec_data']
    for status_count in status_counts:
            with topic.get_producer() as producer:
                    producer.produce(json.dumps(status_count))

zkQuorum, topic = sys.argv[1:]
sc = SparkContext("local[4]", "KafkaprocessCount")
ssc = StreamingContext(sc, 10)
kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
lines = kvs.map(lambda x: x[1])
status_count = lines.map(lambda line: line.split(",")[2]) \
              .map(lambda order_status: (order_status, 1)) \
              .reduceByKey(lambda a, b: a+b)
status_count.pprint()
status_count.foreachRDD(lambda rdd: rdd.foreachPartition(pushprocessStatusInKafka))
ssc.start()
ssc.awaitTermination()