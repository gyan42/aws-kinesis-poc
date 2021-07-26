import glob
import os

import boto3
import aws_kinesis_agg.aggregator
import time
import uuid
import gin
from tqdm import tqdm
import pickle


class KinesisProducer(object):
    def __init__(self,
                 kinesis_region,
                 kinesis_stream_name):
        self._client = boto3.client('kinesis', region_name=kinesis_region)
        self._kinesis_stream_name = kinesis_stream_name
        self._kinesis_agg = aws_kinesis_agg.aggregator.RecordAggregator(max_size=1024*1024)
        self._kinesis_agg.on_record_complete(self.send_record)
        self._early_stop = False
        self._is_continue = True

    def send_record(self,
                    agg_record):
        if agg_record:
            partition_key, _, data = agg_record.get_contents()
            # print(partition_key, data)
            print("Sending the data to AWS Kinesis stream...", self._kinesis_stream_name)
            try:
                if self._early_stop:
                    self._is_continue = False
                self._client.put_record(StreamName=self._kinesis_stream_name,
                                        Data=data,
                                        PartitionKey=partition_key)
            except Exception as e:
                print(e)

    def dump_data(self):
        raise NotImplementedError

    def read_pickle(self, file_name):
        if os.path.exists(file_name):
            ret = pickle.load(open(file_name, 'rb'))
            print(ret)
            return ret
        return {"record_count": -1}

    def write_pickle(self, file_name, data):
        print(data)
        outfile = open(file_name, 'wb')
        pickle.dump(data, outfile)
        outfile.close()

@gin.configurable
class ChicagoCrimeDataset(KinesisProducer):
    def __init__(self,
                 kinesis_region=gin.REQUIRED,
                 kinesis_stream_name=gin.REQUIRED):
        KinesisProducer.__init__(self,
                                 kinesis_region=kinesis_region,
                                 kinesis_stream_name=kinesis_stream_name)

    def dump_data(self):
        path = 'data/*.csv'
        file_name = "failsafe.pickle"
        filenames = glob.glob(path)
        cur_record_count = -1
        fail_safe_book_keeping = self.read_pickle(file_name)

        try:
            print("Files that are going to be streamed...", filenames)
            for filename in filenames:
                print(f"Streaming {filename}...")
                with open(filename, 'r', encoding='utf-8') as data:
                    partition_key = str(uuid.uuid4())
                    for record in tqdm(data):
                        if not self._is_continue:
                            break
                        cur_record_count = cur_record_count + 1
                        self._kinesis_agg.add_user_record(partition_key, record)
                        time.sleep(0.001)
                # Clear out any remaining records that didn't trigger a callback yet
                self.send_record(agg_record=self._kinesis_agg.clear_and_get())
                time.sleep(1)

        except KeyboardInterrupt:
            print('Interrupted')
            fail_safe_book_keeping['record_count'] = cur_record_count
            self.write_pickle(file_name=file_name, data=fail_safe_book_keeping)
            print("Done writing the current record count!", fail_safe_book_keeping)

        # fail_safe_book_keeping['record_count'] = cur_record_count
        # self.write_pickle(file_name=file_name, data=fail_safe_book_keeping)

if __name__ == '__main__':
    gin.parse_config_file('config/producer.gin')
    ChicagoCrimeDataset().dump_data()