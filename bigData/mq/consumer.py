import pika



credentials = pika.PlainCredentials('guest','guest')

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost',credentials=credentials))

channel = connection.channel()

channel.queue_declare(queue='foo',durable=True)


def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)




channel.basic_consume(callback,
                      queue='foo',
                      no_ack=True)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()