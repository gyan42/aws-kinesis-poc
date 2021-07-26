import boto3
from datetime import datetime


def check_if_prefix_exists(prefix):
    client = boto3.client('s3', region_name="Region")
    result = client.list_objects(Bucket="S3-Bucket", Prefix=prefix)
    exists = False
    if "Contents" in result:
        exists = True
    return exists


def lambda_handler(event, context):
    l_client = boto3.client('glue', region_name="Region")
    # Fetching table information from glue catalog
    print("Fetching table info for {}.{}".format(" table-name", "data"))
    try:
        response = l_client.get_table(
            CatalogId="AWS-Account-ID",
            DatabaseName="Database-Name",
            Name="Table-Name"
        )
    except Exception as error:
        print("cannot fetch table")
        exit(1)  # Parsing table info required to create partitions from table
    input_format = response['Table']['StorageDescriptor']['InputFormat']
    output_format = response['Table']['StorageDescriptor']['OutputFormat']
    table_location = response['Table']['StorageDescriptor']['Location']
    serde_info = response['Table']['StorageDescriptor']['SerdeInfo']
    partition_keys = response['Table']['PartitionKeys']
    print(input_format)
    print(output_format)
    print(table_location)
    print(serde_info)
    print(partition_keys)  # Firehose creates partitions in utc time format,
    # creating 2 digit utc time
    hour, day, month, year = str('%02d' % datetime.utcnow().hour) \
        , str('%02d' % datetime.utcnow().day) \
        , str('%02d' % datetime.utcnow().month) \
        , str(datetime.utcnow().year)
    prefix = "year={}/month={}/day={}/hour={}" \
        .format(year, month, day, hour)

    # check if the prefix exist on S3, if not,
    # we don't need to register the partition
    if not check_if_prefix_exists(prefix):
        print("prefix doesn't exist")
        exit(1)

    part_location = "{}year={}/month={}/day={}/hour={}".format(table_location, year, month, day, hour)
    input_dict = {
        'Values': [
            year, month, day, hour
        ],
        'StorageDescriptor': {
            'Location': part_location,
            'InputFormat': input_format,
            'OutputFormat': output_format,
            'SerdeInfo': serde_info
        }
    }
    print(input_dict)
    create_partition_response = l_client.batch_create_partition(
        CatalogId="134867112207",
        DatabaseName="chicago-crimes-batch",
        TableName="chicago_crimes_streaming",
        PartitionInputList=[input_dict]
    )