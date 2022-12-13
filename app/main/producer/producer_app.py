import logging
import time
import json

from event_broadcaster import Broadcaster

dispatcher = Broadcaster()

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
if len(logger.handlers) == 0:
    logger.addHandler(logging.StreamHandler())


def post_event():
    """
    mocking the behaviour of a json stream by pushing events into the kafka producer stream
    """
    input_file = "D:\\kafka_workspaces\\kafka-pyspark\\app\\data\\input\\mock_data.json"
    with open(input_file) as data_file:
        data = json.load(data_file)
        for request_json in data:
            logger.debug("request had the following data: {0}".format(request_json))
            dispatcher.push(request_json)
            # time.sleep(1)


if __name__ == '__main__':
    post_event()
