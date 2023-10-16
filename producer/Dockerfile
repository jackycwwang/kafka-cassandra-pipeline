FROM python:3

# Install librdkafka development headers and other system dependencies
# RUN apt-add-repository 'deb http://packages.confluent.io/$(lsb_release -cs)/7.5 stable main'
RUN apt-get update && apt-get install -y software-properties-common
RUN apt-get update && apt-get install -y librdkafka-dev

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# CMD ["sh", "-c", "python wait_for_mysql.py && python producer.py"]