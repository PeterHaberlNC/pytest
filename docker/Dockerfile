FROM registry.access.redhat.com/ubi9/python-39:1-117.1684741281

ENV username="USR"
ENV password="PWD"
ENV bootstrap_servers=""
ENV topic_name="kafka-vwpoc-topic"
ENV consumer_group="Consumer1"

# Set the working directory in the container
WORKDIR /app

# Copy the content of the local src directory to the working directory
COPY . .

# Install any dependencies
RUN \
  if [ -f requirements.txt ]; \
    then pip install -r requirements.txt; \
  elif [ `ls -1q *.txt | wc -l` == 1 ]; \
    then pip install -r *.txt; \
  fi

# Specify the command to run on container start
CMD [ "python", "aliyun_kafka_consumer.py" ]
