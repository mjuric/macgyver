#!/usr/bin/env python

import pika, sys, uuid, json

task_uuid = str(uuid.uuid4())
cmdline = "./foo bar"

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='tasks', durable=True)

message = json.dumps([task_uuid, cmdline])
channel.basic_publish(
    exchange='',
    routing_key='tasks',
    body=message,
    properties=pika.BasicProperties(
        delivery_mode=2,  # make message persistent
    ))
print(" [x] Sent %r" % message)
connection.close()
