from sparkjobserver.api import SparkJob, build_problems
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import SimpleProducer, KafkaProducer
import sys
from sleep_wake_estimation.getMeanAmplitude import getMeanAmplitude
from sleep_wake_estimation.smoothSW import smoothSW
from sleep_wake_estimation.getSWStatus import getSWStatus
import os
import cPickle as pickle
import numpy as np
import json
with open('/KM_znorm_model.p', 'rb') as f:
    model = pickle.load(f)
XBucket=[]
YBucket=[]
ZBucket=[]
X_Scalar=0
Y_Scalar=0
Z_Scalar=0
BucketCap=32
SW_CAP=8
SW_smWindow=np.ones(SW_CAP)
SLEEPTHRESHOLD = 0.7
WAKETHRESHOLD = 0.3
STIR = True
b=0
LABEL=''
producer = KafkaProducer(bootstrap_servers=os.environ['KAFKA_BROKERS'])
def process(rdd):
    lines = rdd.collect()
    global XBucket
    global YBucket
    global ZBucket
    global X_Scalar
    global Y_Scalar
    global Z_Scalar
    global SW_smWindow
    global b
    global LABEL
    for (u,l) in lines:
     print('[SENSOR_VAL]: '+l)
     i=0
     for n in l.split(','):
      if len(XBucket) == BucketCap:
       print('[Flush]: ' + str(b))
       X_Scalar=getMeanAmplitude(np.asarray(XBucket))
       XBucket=[]
       Y_Scalar=getMeanAmplitude(np.asarray(YBucket))
       YBucket=[]
       Z_Scalar=getMeanAmplitude(np.asarray(ZBucket))
       ZBucket=[]
       X_data = np.array([X_Scalar, Y_Scalar, Z_Scalar])
       print(X_data)
       SW = getSWStatus(X_data.reshape(1,-1), model)
       if SW == 0:
        LABEL = 'SLEEP'
       elif SW == 1:
        LABEL = 'AWAKE'
       mystr = '[SW]: ' + str(SW) + ' ' + LABEL
       print(mystr)
       SW_smWindow = np.append(SW_smWindow,SW)
       SW_smWindow = SW_smWindow[1:]
       SW_sm = smoothSW(SW_smWindow, sleepThreshold=SLEEPTHRESHOLD, wakeThreshold=WAKETHRESHOLD, stir=STIR)
       if SW_sm == 0:
        LABEL = 'SLEEP'
       elif SW_sm == 1:
        LABEL = 'STIR'
       elif SW_sm == 2:
        LABEL = 'AWAKE'
       print('[SW_sm]: '+str(SW_sm) + ' ' + LABEL)
       #Send to Kafka.
       producer.send('mobile',LABEL)
       producer.flush()
       b+=1
      if i == 0:
       XBucket.append(n)
      elif i == 1:
       YBucket.append(n)
      elif i == 2:
       ZBucket.append(n)
      if i < 2:
       i+=1
      else:
       i=0
class SWStatusSparkJob(SparkJob):

    def validate(self, context, runtime, config):
        if config.get('input.strings', None):
            return config.get('input.strings')
        else:
            return build_problems(['config input.strings not found'])

    def run_job(self, context, runtime, data):
        print("!![ML_SPARK_APP_LAUNCHED]!!")
        jData = json.loads(data)
        print("[KAFKA_TOPIC]: " + jData['ktopic'])
        # sc = SparkContext("mesos://zk://10.0.4.219:2181,10.0.4.220:2181,10.0.4.218:2181/mesos",appName="ML")
        # sc = SparkContext(os.environ['MESOS_MASTER_4_SPARK'], appName="SW_STATUS")
        print("[ML]:CTX Created!")
        ssc = StreamingContext(context, 2)
        i = 0
        topic = "monitor"
        topic = jData['ktopic']
        # Send to Kafka.
        producer.send('mobile', '[ML]!')
        producer.flush()
        kvs1 = KafkaUtils.createDirectStream(ssc, [topic], {"bootstrap.servers": os.environ['KAFKA_BROKERS']})

            #"bootstrap.servers": "broker-0.kafka.mesos:9348,broker-1.kafka.mesos:9874,broker-2.kafka.mesos:9972"})

        print("[ML]: Kafka_Stream_Created.")
        kvs1.foreachRDD(process)

        ssc.start()
        print("[ML]:started.")
        ssc.awaitTermination()
        print("[ML]:STOPPED.")
