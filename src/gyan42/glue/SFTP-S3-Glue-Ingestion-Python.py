import boto3
import paramiko
import datetime
from boto3.s3.transfer import TransferConfig

class GlueIngestion(object):
    def __init__(self):
        self.ftp_host = None
        self.ftp_port = None
        self.ftp_username = None
        self.ftp_password = None
        self.ftp_file_path = None
        self.s3_bucket = None
        self.last_etl_execution_time = None

    def get_credentials_n_params(self):
        # Fetch credentials from SSM
        print("fetching credentials from SSM!")
        self.ssm = boto3.client('ssm', region_name="Region")
        try:
            self.ftp_host = self.ssm.get_parameter(Name='ftp_host')['Parameter']['Value']
            self.ftp_port = self.ssm.get_parameter(Name='ftp_port')['Parameter']['Value']
            self.ftp_username = self.ssm.get_parameter(Name='ftp_username')['Parameter']['Value']
            self.ftp_password = self.ssm.get_parameter(Name='ftp_password')['Parameter']['Value']
            self.ftp_file_path = self.ssm.get_parameter(Name='ftp_file_path')['Parameter']['Value']
            self.s3_bucket = self.ssm.get_parameter(Name='s3_bucket')['Parameter']['Value']
            self.last_etl_execution_time = self.ssm.get_parameter(Name='Last_ETL_Execution_Time')['Parameter']['Value']
        except Exception as error:
            print("Error fetching parameter from parameter store, ", error)
            exit(1)
        print("parameters fetched successfully")

    def connect_file_server(self):
        # Establishing connection with file server
        print("Connecting to file server!")
        try:
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(
                paramiko.AutoAddPolicy())
            self.ssh_client.connect(hostname=self.ftp_host,
                                    username=self.ftp_username,
                                    password=self.ftp_password,
                                    port=self.ftp_port)
        except Exception as error:
            print("Error occurred while connecting to ftp server: ", error)
            exit(1)
        print("Established connection to file server successfully")

    def upload_to_s3(self):
        s3 = boto3.client('s3', region_name="Region")
        MB = 1024 ** 2
        config = TransferConfig(multipart_threshold=100 * MB,
                                multipart_chunksize=10 * MB)
        sftp = self.ssh_client.open_sftp()
        last_etl_execution_time = datetime.datetime.strptime(self.last_etl_execution_time, '%Y-%m-%d %H:%M:%S')

        '''
        Iterating over file server directory and comparing last modified date of file
        We will be uploading those files whose last_modified_date is >= last_etl_execution_time of python shell job
        '''
        print("Iterating over files on file server!")
        for file in sftp.listdir(self.ftp_file_path):
            file_path = self.ftp_file_path + "/" + file
            utime = sftp.stat(file_path).st_mtime
            last_modified = datetime.datetime.fromtimestamp(utime)
            if last_modified >= last_etl_execution_time:
                file_to_upload = sftp.file(file_path, mode='r')
                try:
                    print("Uploading File %s" % file)
                    s3.upload_fileobj(file_to_upload, self.s3_bucket, file, Config=config)
                except Exception as e:
                    print("Failed to upload %s to %s: %s" % (file, '/'.join([self.s3_bucket]), e))
        print("Uploaded files on S3 successfully")

        # updating last_ETL_Running_Job_Time in SSM Parameter store
        print("Updating last_ETL_Running_Job_Time in SSM Parameter store")
        try:
            self.ssm.put_parameter(Name='Last_ETL_Execution_Time',
                                   Value=str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                                   Type='String', Overwrite=True)
        except self.ssm.exceptions as error:
            print("Error fetching parameter from parameter store, ", error)
            exit(1)
        print("Job completed successfully!!!")
