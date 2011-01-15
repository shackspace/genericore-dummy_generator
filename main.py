#!/usr/bin/env python2
import pika,argparse
import json, sys,time
import logging

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger('dummy_generator')

parser = argparse.ArgumentParser(description='generates dummy package on given exchange against AMQP')
parser.add_argument('--host',default='141.31.8.11',      help='AMQP host ip address')
#parser.add_argument('--port',type=int,default=5672,      help='AMQP host port')
parser.add_argument('-u','--username',default='guest',   help='AMQP username') 
parser.add_argument('-p','--password',default='guest',   help='AMQP password') 
parser.add_argument('-e','--exchange',default='mail_src',help='AMQP Exchange to publish in') 

parser.add_argument('-f','--file',default=sys.stdin,type=argparse.FileType('r'),help='payload to send to exchange, default is STDIN') 

parser.add_argument('-t','--timeout',type=int,default=9999999,help='timeout in seconds between generation cycles') 
args = parser.parse_args()
log.info ("Parameters %s" % args)

connection = pika.AsyncoreConnection(pika.ConnectionParameters(
          credentials = pika.PlainCredentials(args.username,args.password), 
          host=args.host))
channel = connection.channel()

channel.exchange_declare(exchange=args.exchange,
                             type='fanout')


log.info('Reading payload from file')
data = args.file.read()

while True:
  log.info('Publishing to exchange "%s" ,payload : %s' % (args.exchange,data))
  channel.basic_publish(exchange=args.exchange,
      routing_key='',
      body=data)
  log.info('Waiting %d for next publish' %args.timeout)
  time.sleep(args.timeout)


pika.asyncore_loop()
