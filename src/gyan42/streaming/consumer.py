import base64
import time

import boto3
# from boto3.kinesis.exceptions import ProvisionedThroughputExceededException
import datetime
import gin
from aws_kinesis_agg.deaggregator import deaggregate_records


def process(kpl_records):
    output = []
    for kpl_record in kpl_records:
        recordId = kpl_record['recordId']
        records_deaggregated = deaggregate_records(kpl_record)
        decoded_data = []
        for kpl_record in records_deaggregated:
            data = base64.b64decode(
                kpl_record['kinesis']['data']) \
                .decode('utf-8')
            decoded_data.append(data)
        output_data = "".join(decoded_data)
        output_record = {
            'recordId': recordId,
            'result': 'Ok',
            'data': base64.b64encode(output_data.encode('utf-8')).decode('utf-8')
        }
        output.append(output_record)
    return output


class KinesisConsumer:
    """Generic Consumer for Amazon Kinesis Streams"""
    def __init__(self,
                 kinesis_stream_name,
                 kinesis_region,
                 shard_id='shardId-000000000000',
                 iterator_type='TRIM_HORIZON',#TRIM_HORIZON',#'LATEST',
                 worker_time=300,
                 sleep_interval=0.5):

        self._client = boto3.client('kinesis', region_name=kinesis_region)
        self.stream_name = kinesis_stream_name
        self.shard_id = str(shard_id)
        self.iterator_type = iterator_type
        self.worker_time = worker_time
        self.sleep_interval = sleep_interval

    def process_records(self, records):
        """the main logic of the Consumer that needs to be implemented"""
        raise NotImplementedError

    @staticmethod
    def iter_records(records):
        for record in records:
            part_key = record['PartitionKey']
            data = record['Data']
            yield part_key, data

    def run(self):
        print("Run....")
        """poll stream for new records and pass them to process_records method"""
        response = self._client.get_shard_iterator(StreamName=self.stream_name,
                                                   ShardId=self.shard_id,
                                                   ShardIteratorType=self.iterator_type)

        print(response)

        next_iterator = response['ShardIterator']

        start = datetime.datetime.now()
        finish = start + datetime.timedelta(seconds=self.worker_time)

        while finish > datetime.datetime.now():
            try:
                response = self._client.get_records(ShardIterator=next_iterator, Limit=25)

                print(response)

                records = response['Records']

                # print(process(records))

                if records:
                    self.process_records(records)

                next_iterator = response['NextShardIterator']
                time.sleep(self.sleep_interval)
            except Exception as e:
                print("error "* 10)
                print(e)
                time.sleep(1)

@gin.configurable
class EchoConsumer(KinesisConsumer):
    def __init__(self,
                 kinesis_region=gin.REQUIRED,
                 kinesis_stream_name=gin.REQUIRED,
                 shard_id='shardId-000000000000',
                 iterator_type='LATEST',
                 worker_time=300,
                 sleep_interval=0.5):
        KinesisConsumer.__init__(self,
                                 kinesis_stream_name,
                                 kinesis_region,
                                 shard_id,
                                 iterator_type,
                                 worker_time,
                                 sleep_interval)

    """Consumers that echos received data to standard output"""
    def process_records(self, records):
        """print the partion key and data of each incoming record"""
        print("process_records", "^" * 100)
        for part_key, data in self.iter_records(records):
            print("*" * 100)
            print("*" * 100)
            # print(part_key, ":", data)
            # print("*" * 100)

        # print(records)



if __name__ == '__main__':
    gin.parse_config_file('config/consumer.gin')
    EchoConsumer().run()