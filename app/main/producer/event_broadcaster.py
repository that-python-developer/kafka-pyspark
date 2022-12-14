import json
import os
import time
import logging

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


class Broadcaster:

    def __init__(self, kafka_servers, kafka_topic):
        """
        Initial set up for the Kafka Producer
        """
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        self.logger = logger
        self.kafka_servers = kafka_servers
        self.kafka_topic = kafka_topic

        while not hasattr(self, 'producer'):
            try:
                self.producer = KafkaProducer(bootstrap_servers=self.kafka_servers)
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
                    self.kafka_topic,
                    bytes(json.dumps(message).encode('utf-8'))
                )
        except AttributeError:
            self.logger.error(
                "Unable to send {0}. The producer does not exist.".format(message)
            )
