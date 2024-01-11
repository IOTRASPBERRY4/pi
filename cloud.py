import pika
from line_notify import LineNotify
from time import gmtime, strftime

import pymongo

import json
import time
from datetime import datetime
from influxdb_client import InfluxDBClient, Point, WritePrecision, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS

#CAU HINH LINE NOTIFY
#MA TRUY CAP LINE NOTIFY 1.
ACCESS_TOKEN = "ClypwkTdGlVxjzHAo6ri1XeHgjWDRcNrnShbEdmq7ND"
notify = LineNotify(ACCESS_TOKEN)


# CAI DAT CAU HINH CAC BIEN KET HOP VOI INFLUXDB
# MA TRUY CAP CHO INFLUXDB
token = "bnxV0WO6lM7AWf4S9QQKL4p2kmypNxRfAOg5vl_b6Du6ECbZZijJj0yHKZ8_xd6QFCICso90eJkpBek0VQLjlw=="
# NAME TO CHUC CUA INFLUXDB
org = "42dced558f814cdc"
bucket = "WATER"
# DIA CHI CUA INFLUXDB
address_influxdb = "http://ec2-54-252-162-194.ap-southeast-2.compute.amazonaws.com:8086"
_client = InfluxDBClient(address_influxdb , token=token)

_write_client = _client.write_api(write_options=WriteOptions(batch_size=500,
                                                             flush_interval=10_000,
                                                             jitter_interval=2_000,
                                                             retry_interval=5_000,
                                                             max_retries=5,
                                                             max_retry_delay=30_000,
                                                             exponential_base=2))

# CAU HINH CAC BIEN KET HOP VOI MONGODB
# DIA CHI MONGODB
address_mongodb = "mongodb://ec2-54-253-242-163.ap-southeast-2.compute.amazonaws.com:27017/"
myclient = pymongo.MongoClient(address_mongodb)
# KHAI BAO DOI TUONG DATABASE DUOC USER KHOI TAO
mydb = myclient["auto_config"]
# KHAI BAO DOI TUONG TRONG DATABASE O TREN
mycol = mydb["auto_salatiny"]
# HAM NHAN DU LIEU TU MONGODB
def recive_mongodb():
    x = mycol.find_one()
    s = str(x)
    s = s.replace("\'", "\"")
    auto = json.loads(s)
    return auto
# HAM SO SANH GIA TRI TU RABBITMQ VOI GIA TRI TREN MONGODB
def max_salatiny(doman):
    salatiny_max = recive_mongodb()
    if doman > salatiny_max["salatiny"]:
        print(salatiny_max["salatiny"])
        return True
    return False
    
measurement = "Water"
# HAM GIAI MA CAC GOI TIN TU RABBIT VA GUI CAC LEN INFLUXDB
def queue1_callback(ch, method, properties, body):
  print(" [x] Received queue 1: %r" % body)
  # LAY THOI GIAN HIEN TAI
  now = datetime.now()
  timestamp_aq = datetime.timestamp(now)
  iso = datetime.utcnow()
  # CHUYEN DOI DU LIEU THANH CHUOI JSON
  nhietdo = json.loads(body)
  # GUI DU LIEU LEN INFLUXDB
  _write_client.write(bucket, org, [{"measurement": measurement, "fields": {"Temperature": float(str(nhietdo))},
                                     "time": iso}])
# HAM GIAI MA CAC GOI TIN TU RABBIT VA GUI CAC LEN INFLUXDB
def queue2_callback(ch, method, properties, body):
  print(" [x] Received queue 2: %r" % body)
  # LAY THOI GIAN HIEN TAI
  now = datetime.now()
  timestamp_aq = datetime.timestamp(now)
  iso = datetime.utcnow()
  # CHUYEN DOI DU LIEU THANH CHUOI JSON
  doman = json.loads(body)
  # GUI DU LIEU LEN INFLUXDB
  _write_client.write(bucket, org, [{"measurement": measurement, "fields": {"Salinity": float(str(doman))},
                                     "time": iso}])
  # SO SANH DO MAN VOI GIA TRI CUA MOGODB
  if max_salatiny(doman):
      print(max_salatiny(doman))
      # GUI THONG BAO QUA LINE NOTIFY
#       notify.send("Warning!! Salatiny dang tang cao %r" % doman + ' ' +str(strftime("%d/%m/%y %H:%M:%S")))
      notify.send("Warning!! Salatiny dang tang cao " + ' ' + ((f"{doman:.2f}").format(doman)) +" (ppm) "+ ' ' +str(strftime("%d/%m/%y %H:%M:%S")))
      # GUI TIN NHAN VE HANG DOI RABBIT MQ
      rabbit_1()
  else:
      rabbit_0()

def on_open(connection):
  connection.channel(on_open_callback = on_channel_open)
  
def on_channel_open(channel):
  channel.basic_consume('nhiet_do', queue1_callback, auto_ack = True)
  channel.basic_consume('do_man', queue2_callback, auto_ack = True)
    
#KHOI TAO DIA CHI, PORT, USER DE KET NOI VOI RABBIT MQ SERVER
credentials = pika.PlainCredentials('guest', 'guest')
address_rabbit= 'ec2-54-206-100-244.ap-southeast-2.compute.amazonaws.com'
parameters = pika.ConnectionParameters(address_rabbit, 5672, '/', credentials)
# KET NOI VOI RABBIT MQ
connection = pika.SelectConnection(parameters = parameters, on_open_callback = on_open)

# KHOI TAO HANG DOI DE GUI TIN NHAN VE RABBIT MQ
connection_1 = pika.BlockingConnection(parameters)
channel = connection_1.channel()
channel.queue_declare(queue='True')

#HAM KHOI TAO DU LIEU GUI LEN RABBITMQ
def rabbit_1():
    data = json.dumps(1)
    channel.basic_publish(exchange= '', routing_key='True', body = data)
def rabbit_0():
    data = json.dumps(0)
    channel.basic_publish(exchange= '', routing_key='True', body = data)

try:
  connection.ioloop.start()
except KeyboardInterrupt:
  connection.close()
  connection.ioloop.start()



