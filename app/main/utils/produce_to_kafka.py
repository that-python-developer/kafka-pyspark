import time
import json


def produce_to_kafka(dispatcher, logger, input_file):
    with open(input_file) as data_file:
        data = json.load(data_file)
        for request_json in data:
            logger.debug("request had the following data: {0}".format(request_json))
            dispatcher.push(request_json)
            # time.sleep(1)
