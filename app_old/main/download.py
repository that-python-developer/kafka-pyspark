import requests
import json

from datetime import datetime


def download_file():
    current_time = datetime.now().strftime('%Y_%M_%d-%H_%M_%S')
    data_directory = "D:\\kafka_workspaces\\kafka-pyspark\\app_old\\data"
    github_events_api = "https://api.github.com/events"

    res = requests.get(github_events_api)
    with open(f"{data_directory}\\{current_time}.json", "w+") as file:
        json.dump(json.loads(res.content), file)
    return res


if __name__ == '__main__':
    download_file()
