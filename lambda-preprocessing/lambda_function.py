import base64
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

def main(event, context):
    """

    Args:
        event:
        context:

    Returns:
        {'recordId': ..., 'result': 'ok', 'data': ...}
    """

    kpl_records = event['records']
    output = print(kpl_records)
    print("Successfully processed \
            {} records.".format(len(event['records'])))
    return {'records': output}

if __name__ == "__main__":
    main('', '')