import logging

from event_broadcaster import Broadcaster

from app.main.config import *
from app.main.utils.produce_to_kafka import produce_to_kafka


kafka_topic = DATA_STREAM_ANALYSIS
kafka_servers = KAFKA_SERVERS

dispatcher = Broadcaster(kafka_servers, kafka_topic)

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
if len(logger.handlers) == 0:
    logger.addHandler(logging.StreamHandler())


def post_event():
    """
    mocking the behaviour of a json stream by pushing events into the kafka producer stream
    """
    input_file = "D:\\kafka_workspaces\\kafka-pyspark\\app\\data\\input\\mock_data.json"
    produce_to_kafka(dispatcher, logger, input_file)


if __name__ == '__main__':
    post_event()
