from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import os
import time
import logging

kafka_topic = os.environ.get('PCDEMO_CHANNEL') or 'data-stream-analysis'
kafka_servers = '192.168.3.214:29093'


class Broadcaster:

    def __init__(self):
        """
        Initial set up for the Kafka Producer
        """
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        self.logger = logger

        while not hasattr(self, 'producer'):
            try:
                self.producer = KafkaProducer(bootstrap_servers=kafka_servers)
            except NoBrokersAvailable as err:
                self.logger.error("Unable to find a broker: {0}".format(err))
                time.sleep(1)

    def push(self, message):
        """

        :param message: pushing messages to the kafka topics
        :return: None
        """
        self.logger.info("Publishing: {0}".format(message))
        try:
            if self.producer:
                self.producer.send(
                    kafka_topic,
                    bytes(json.dumps(message).encode('utf-8'))
                )
        except AttributeError:
            self.logger.error(
                "Unable to send {0}. The producer does not exist.".format(message)
            )
