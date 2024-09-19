import pika
import os
import "Producer-And-Consumer\producer\producer_interface.py"

class mqProducer(mqProducerInterface):
    def __init__(self, routing_key: str, exchange_name: str) -> None:
        self.routing_key = routing_key
        self.exchange_name = exchange_name
        setupRMQConnection()

    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        connection = pika.BlockingConnection(parameters=con_params)
        
        # Establish Channel
        channel = connection.channel()
        # Create the exchange if not already present
        exchange = channel.exchange_declare(exchange=self.exchange_name)

    def publishOrder(self, message: str) -> None:
        # Basic Publish to Exchange
        channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=self.routing_key,
            body="Message",
        )

        # Close Channel
        channel.close()

        # Close Connection
        connection.close()
    