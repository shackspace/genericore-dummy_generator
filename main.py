#!/usr/bin/env python2
import pika,argparse
import json, sys,time
import logging

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger('dummy_generator')

parser = argparse.ArgumentParser(description='generates dummy package on given exchange against AMQP')
parser.add_argument('--host',default='141.31.8.11',      help='AMQP host ip address')
parser.add_argument('--port',type=int,default=5672,      help='AMQP host port')
parser.add_argument('-u','--username',default='shack',   help='AMQP username') 
parser.add_argument('-p','--password',default='guest',   help='AMQP password') 
parser.add_argument('-e','--exchange',default='mail_src',help='AMQP Exchange to publish in') 

parser.add_argument('-f','--file',default=sys.stdin,type=argparse.FileType('r'),help='payload to send to exchange, default is STDIN') 

parser.add_argument('-t','--type',default='fanout',help='AMQP Exchange type') 
parser.add_argument('-b','--heartbeat',type=int,default=0,help='AMQP Heartbeat value') 
parser.add_argument('-v','--vhost',default='/',help='AMQP vhost definition') 

parser.add_argument('-r','--repeat',type=int,default=0,help='repeat message after N seconds cycles') 
args = parser.parse_args()
log.info ("Parameters %s" % args)

connection = pika.AsyncoreConnection(pika.ConnectionParameters(
          credentials = pika.PlainCredentials(args.username,args.password), 
          host=args.host, port=args.port,
          heartbeat=args.heartbeat,
          virtual_host=args.vhost,
          ))
channel = connection.channel()

channel.exchange_declare(exchange=args.exchange,
                             type=args.type)


log.info('Reading payload from file')
data = args.file.read()

while True:
  log.info('Publishing to exchange "%s" ,payload : %s' % (args.exchange,data))
  channel.basic_publish(exchange=args.exchange,
      routing_key='',
      body=data)
  if args.repeat == 0:
     exit(0)
  log.info('Waiting %d for next publish' %args.repeat)
  time.sleep( args.repeat)

