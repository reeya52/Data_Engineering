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
# Consume messages from Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from confluent_kafka import Consumer
import json
import ccloud_lib
import zlib
from google.cloud import storage
import rsa
import os

publicKey, privateKey = rsa.newkeys(512)

def data_compress(data):
    compress = zlib.compressobj(zlib.Z_DEFAULT_COMPRESSION, zlib.DEFLATED, -15)
    data = json.dumps(data)
    data = data.encode('utf-8')
    compressed_data = compress.compress(data)
    compressed_data += compress.flush()

    return compressed_data

def data_encrypt(data):
    encMessage = rsa.encrypt(data.encode(), publicKey)

    return encMessage

if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    consumer_conf['group.id'] = 'archive_consumer'
    consumer_conf['auto.offset.reset'] = 'earliest'
    consumer = Consumer(consumer_conf)

    # Subscribe to topic
    consumer.subscribe([topic])

    # Process messages
    total_count = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming

                if os.path.exists("temp.txt"):
                    client = storage.Client.from_service_account_json(json_credentials_path='de-project-spr22-eb97ba23263c.json')
                    bucket = client.get_bucket('de_activity_bucket')
                    file_name = os.path.abspath('temp.txt')
                    blob_object = bucket.blob(file_name)
                    blob_object.upload_from_filename(file_name)

                    print('record stored in bucket')

                    if os.path.exists("temp.txt"):
                        os.remove("temp.txt")
                        print("File deleted")
                    else:
                        print("File does not exist")
                else:
                    continue

                print("Waiting for message or event/error in poll()")
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                # Check for Kafka message
                record_key = msg.key()
                record_value = msg.value()
                data = json.loads(record_value)
                compressed_data = data_compress(data)
                encrypted_data = data_encrypt(compressed_data)
                
                if os.path.exists("temp.txt"):
                    f = open('temp.txt', 'ab')
                    f.write(encrypted_data)
                else:
                    f = open('temp.txt', 'wb')
                    f.write(encrypted_data)

                f.close()

                print("Record loaded into file")

                count = data['count']
                total_count += count
                print("Archived record with key {} and value {}, \
                      and updated total count to {}"
                      .format(record_key, record_value, total_count))
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
