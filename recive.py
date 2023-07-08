import pip
pip.main(["install", "pika"])
import pika
import json
import time
# import Adafruit_DHT#Khai bao su dung ham trong thu vien
# import sys
# sensor = Adafruit_DHT.DHT11	
# gpio = 27
from time import sleep, strftime

address_rabbit = 'ec2-52-63-11-162.ap-southeast-2.compute.amazonaws.com'
credentials = pika.PlainCredentials('guest', 'guest')
parameters = pika.ConnectionParameters( address_rabbit ,5672,'/',credentials)

connection = pika.BlockingConnection(parameters)

channel = connection.channel()
#channel1 = connection.channel()

channel.queue_declare(queue='time')
#channel1.queue_declare(queue='do_am')

while True:
    #humidity, temperature = Adafruit_DHT.read(sensor, gpio)
#DOI KHI CO THE KHONG DOC DUOC DU LIEU TU CAM BIEN VI VAY CAN KIEM TRA DU LIEU.
#DOC DUOC CO HOP LE HAY KHONG.
    #if humidity is not None and temperature is not None:
#while True:
        #print("Nhiet do: " + str (temperature) + " *C " + " Do am: " + str (humidity) + " % ")
    #humidity, temperature = Adafruit_DHT.read_retry(sensor, gpio)
        #print("NHIET DO: {0:0.2f},DO AM: {1:0.2f}".format(temperature, humidity))
        #@data = {'temperature':int(temperature)}
        #data=json.dumps(data)
    data = json.dumps(strftime("%d/%m/%y %H:%M:%S"))#json.dumps (temperature)# + ' ' + str(strftime("%d/%m/%y %H:%M:%S"))
        #data1 = json.dumps (humidity)# + ' ' + str(strftime("%d/%m/%y %H:%M:%S"))
        #print('NHIETDO = {0:0.01f}*C  DOAM = {1:0.01f}%'.format(temperature, humidity))
    channel.basic_publish(exchange='',routing_key='time',body= data)
        #channel1.basic_publish(exchange='',routing_key='do_am',body= data1)
       #$ print(" [x] Nhiet do = " + temperature)
    print(" [x] Time " + data)
    # Tính kích thước của message
    size = len(data.encode('utf-8'))

# In kích thước của message
    print("Kích thước của message là:", size, "byte")
    print("Da thay doi ")

    
        #print(" [x] DO AM = " + data1)
    #else:
     #   print("THIET BI LOI, HAY THU LAI")			#Bao loi thiet bi cam bien
     #time.sleep(5)
connection.close()


