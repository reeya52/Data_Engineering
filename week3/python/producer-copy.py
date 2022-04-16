#!/usr/bin/env python
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# =============================================================================
#
# Produce messages to Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from confluent_kafka import Producer, KafkaError
import json
import ccloud_lib
import time
import random


if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Producer instance
    producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    producer = Producer(producer_conf)

    # Create topic if needed
    ccloud_lib.create_topic(conf, topic)

    delivered_records = 0

    sent_records = 0

    # Optional per-message on_delivery handler (triggered by poll() or flush())
    # when a message has been successfully delivered or
    # permanently failed delivery (after retries).
    def acked(err, msg):
        global delivered_records
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            delivered_records += 1
            print("Produced record to topic {} partition [{}] @ offset {}"
                    .format(msg.topic(), msg.partition(), msg.offset()))
           #time.sleep(0.25)

    with open('sample.json', 'r') as f:
        data = json.load(f)

    #print(data)

    #for n in range(100):
    for record in data:
        record_key = str(random.randint(1,5))
        #print(record_key)
        #record_value = json.dumps({'count': n})
        record_value = json.dumps(record)
        print("Producing record: {}\t{}".format(record_key, record_value))
        #print("producing record")
        producer.produce(topic, key=record_key, value=record_value, on_delivery=acked)
        sent_records += 1
        # p.poll() serves delivery reports (on_delivery)
        # from previous produce() calls.
        producer.poll(0)
        #producer.flush()
        #print("Sleeping")
        #time.sleep(0.25)
        #print("Wakeing up")
        if sent_records % 5 == 0:
            time.sleep(2)
        if sent_records % 15 == 0:
            producer.flush()

    #producer.flush()

    print("{} messages were produced to topic {}!".format(delivered_records, topic))
