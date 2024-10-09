import logging.config
import sys, os, re, boto3, pathlib, logging
from boto3.session import Session,Config

modulesPath = os.path.join(pathlib.Path(__file__).resolve().parents[2], "modules")
sys.path.append(modulesPath)

from modules.log_config.log_config import LoggingConf

loggingConf = LoggingConf().config
logging.config.dictConfig(loggingConf)
logger = logging.getLogger(__name__)

logger.info("Initializing upload module...")

# 'sources' should come from main onfig !!!
sourcesBasePath = os.path.join(pathlib.Path(__file__).resolve().absolute().parents[2],'sources')


# All of the following should come from main config
endpoint='http://ip:9020'  
key = ''
secret = ''
bucketName = 'airports'

class Upload(object):
    
    def __init__(self, key = key, secret = secret, endpoint = endpoint, bucketName = bucketName, sourcesBasePath = sourcesBasePath, fileNames = None):

        boto3.DEFAULT_SESSION

        self.endpoint = endpoint
        self.key = key
        self.secret = secret
        self.bucketName = bucketName
        self.sourcesBasePath = sourcesBasePath
        # Create sourceBasPath if it does not exist
        os.path.exists(self.sourcesBasePath) or os.makedirs(self.sourcesBasePath)

        # If fileNames is not set then set to the list of files under sourcesBasePath
        self.fileNames = os.listdir(self.sourcesBasePath) if not fileNames else fileNames
  
        #### Creating a session with resource
        conf = Config(signature_version='s3v4', s3={'addressing_style': 'path',},
                            connect_timeout = 3,
                            read_timeout = 3,
                            retries = {
                            'max_attempts': 2,
                            'mode': 'standard'
                            })
    
        session = Session(aws_access_key_id=key, aws_secret_access_key=secret)  
        self.s3 = session.resource('s3', endpoint_url=endpoint, use_ssl=False, verify=False, config=conf)

        #### Creating a session client
        self.client = boto3.client('s3', use_ssl=False, verify=None, endpoint_url=endpoint,
                aws_access_key_id=key, aws_secret_access_key=secret, 
                aws_session_token=None, 
                config=conf)


        self.bucket = self.s3.Bucket(bucketName)

        self.transferConfig=boto3.s3.transfer.TransferConfig(multipart_threshold=8388608, max_concurrency=10, multipart_chunksize=8388608, num_download_attempts=5, max_io_queue=100, io_chunksize=262144, use_threads=True)

    def create_bucket(self):

        logger.info("Creating the bucket...")
        bucketFound = False
        response = self.client.list_buckets()

        for bucket in response['Buckets']:
            if bucket['Name'] == self.bucketName:
                bucketFound = True
                print(f"Bucket {self.bucketName} exists.")
                break

        if not bucketFound:
            response = self.client.create_bucket(
                    Bucket=self.bucketName,
                    ObjectOwnership = 'BucketOwnerEnforced'
            )

            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                print(f"Bucket {self.bucketName} was created successfully.")
            else:
                print(f"Bucket {self.bucketName} could not be created!")
                sys.exit("Quiting...")


    def upload_files(self):

        logger.info("Uploading files...")
        for fileName in self.fileNames:

            sourcePath = os.path.join(self.sourcesBasePath, fileName)
            parentDir = fileName.rstrip("csv").rstrip(".")
            uploadDirectoryPath = 'raw/' + parentDir + "/"
            key = uploadDirectoryPath + fileName

            print(f"=== Uploading {sourcePath} ==========")

            # Create a directory object for each file uploaded
            response = self.client.put_object(Bucket=self.bucketName, Key=uploadDirectoryPath, ContentType='application/x-directory')
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                print(f"Folder {uploadDirectoryPath} was created.")
            else:
                print(f"Folder {uploadDirectoryPath} could not be created.")

            # Upload files
            try:
                self.client.upload_file(Bucket = self.bucketName, Filename = sourcePath, 
                                               Key=key, Config = self.transferConfig)
                print(f"File {key} was uploaded.")
            except Exception as err:
                print(f"File {key} could not be created.")
                print(err)

if __name__ == '__main__':

    uploader = Upload()
    uploader.create_bucket()
    # uploader.upload_files()


