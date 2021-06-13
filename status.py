#!/usr/bin/env python

import pika, sys, json, socket, psutil, time, threading

task_uuid, cmdline = None, None

def status_reporter():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='status', durable=True)

    ip = socket.gethostbyname(socket.gethostname())

    try:
        while True:
            ram, cpu = psutil.virtual_memory().percent, psutil.cpu_percent()
            message = json.dumps([ip, task_uuid, ram, cpu])

            channel.basic_publish(
                exchange='',
                routing_key='status',
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # make message persistent
                ))
            print(f"{ip=}, {task_uuid=}, {ram=}, {cpu=}, {cmdline=}")
            time.sleep(5)
    finally:
        connection.close()

def task_receiver():

    def _executor(ch, method, properties, body):
        global task_uuid, cmdline
        task_uuid, cmdline = json.loads(body)

        #... execute task ...
        print(cmdline)
        time.sleep(50)
        
        ch.basic_ack(delivery_tag=method.delivery_tag)
        task_uuid = cmdline = None

    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='tasks', durable=True)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='tasks', on_message_callback=_executor)
    channel.start_consuming()


if __name__ == "__main__":
    threading.Thread(target=status_reporter, args=(), daemon=True).start() # status receiver thread
    task_receiver()
