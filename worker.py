#!/usr/bin/env python

#
#  queues:
#    commands: -> (uuid:text, cmd:text)
#    status: <- (what:text)
#                  <- (what=status, cmd_running=None|uuid, ram_usage=..., memory_usage=...)
#
#
# Submit utility:
#  Submit message to the queue
#
# Server control loop:
#  while True:
#    - get number of messages on queue
#    - get number of nodes in fleet for this job (w. the right label)
#    - if number of nodes < number of messages on queue, scale up to min(n_messages, 30)
#      - launch node with the given cluster label, metadata for startup script
#    - if node is disconnected or jobless from rmq for longer than X minutes, delete it
#      - https://stackoverflow.com/questions/13037121/in-pika-or-rabbitmq-how-do-i-check-if-any-consumers-are-currently-consuming
#
# Worker control loop:
#
#  startup parameters: (rmq coordinates, local username, queue name)
#
#  setuid to $username
#  connect to rmq coordinates, named queue
#
#  while True:
#    - Consume a new task on the queue
#    - Launch and wait for it to finish.
#    - Acknowledge
#
#  separate thread:
#    - If idle for longer than X seconds, terminate instance
#
# See here for how to write this:
#  https://github.com/pika/pika/blob/0.12.0/examples/basic_consumer_threaded.py
#

import pika
import time

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='tasks', durable=True)
print(' [*] Waiting for messages. To exit press CTRL+C')


def callback(ch, method, properties, body):
    print(" [x] Received %r" % body.decode())
    time.sleep(10)
    print(" [x] Done")
    exit()
#    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='tasks', on_message_callback=callback)

channel.start_consuming()
