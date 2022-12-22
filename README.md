### Setting up the project

1. Setup the project (pull into your local)
2. Create a venv
3. Run `pip install -r requirements.txt`

### Setting up and getting the Docker up and running

1. Download docker from - https://www.docker.com/products/docker-desktop/
2. Complete the docker setup
3. Run docker desktop application in your local 
4. In the **docker-compose.yml** file in the project replace the ip-address with your local machine ip address
5. To get the local machine IP run `ipconfig` command in cmd 
6. In the cmd run `docker-compose up -d`

You can see your docker running in the docker desktop application.

### Running the producer

1. Run the **app/main/producer/mock_data_producer_app.py** file from the project
2. This will read the data from **app/data/input/mock_data.json** and produce messages in a Kafka topic (which is configured in the **app/main/config.py** file)

### Running the consumer

1. Run the **app/main/consumer/mock_data_consumer_app.py** file from the project
2. This will read the data from the Kafka topic in a PySpark dataframe and save results to multiple csv (app/data/output/mock_data)
3. One of the method will write the result to another Kafka topic

### Setting up Offset Explorer

1. This is a free tool to get an overview of our Kafka cluster
2. Download it from - https://www.kafkatool.com/download.html
3. Create a new connection and set the variables as follows -
   1. Cluster name - can be anything
   2. Zookeeper Host - localhost
   3. Zookeeper Port - 22181
   4. Under Advanced -> Bootstrap servers - <your-ip>:29093

Whenever you create a topic while running our project that topic can be seen here and also the number of messages in that topic.

#### You can also check the messages in a Kafka topic using the following approach - 

1. Open PowerShell of CMD
2. Run `docker ps`
3. Run `docker exec -it kafka-kafka-1 bash` where kafka-kafka-1 is the name of the docker 
4. Run `cd /bin`
5. To consume from a Kafka topic -
`kafka-console-consumer --bootstrap-server <your-ip>:29093 --topic data-stream-output --from-beginning`