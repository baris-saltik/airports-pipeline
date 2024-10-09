import os, sys, pathlib, subprocess, logging
import logging.config

# Add modules to sys.path
sys.path.append(os.path.join(pathlib.Path(__file__).resolve().absolute().parents[2], 'modules'))

from log_config.log_config import LoggingConf

# Set logging
loggingConfig = LoggingConf()
logging.config.dictConfig(loggingConfig.config)
logger = logging.getLogger(__name__)

logger.info("Initializing download module...")

# Defaults
baseWebPath = "https://davidmegginson.github.io/ourairports-data/"
fileNames = ["airports.csv", "airport-frequencies.csv", "runways.csv", "navaids.csv", "countries.csv", "regions.csv"]
sourcesBasePath = "sources"


class Download(object):

    def __init__(self, baseWebPath = baseWebPath, sourcesBasePath = sourcesBasePath, fileNames = fileNames ):

        self.baseWebPath = baseWebPath
        self.fileNames = fileNames
        self.filesDownloaded = False

        logger.debug("#####" + self.baseWebPath + "###########")
        self.baseDowloadPath = os.path.join(pathlib.Path(__file__).resolve().absolute().parents[2], sourcesBasePath)
        

        try:
            (os.path.exists(self.baseDowloadPath) and os.path.isdir(self.baseDowloadPath)) or os.makedirs(name = self.baseDowloadPath)
        except Exception as err:
            logger.critical(f"Cannot find or create {self.baseDowloadPath}, aborting...")
            sys.exit()
    
    def download_files(self):
        
        for _fileName in self.fileNames:
            logger.debug(f"Downloading {_fileName}...")
            _webPath = self.baseWebPath + _fileName
            _downloadPath = os.path.join(self.baseDowloadPath, _fileName)

            # Clean up existing files
            try:
                if os.path.exists(_downloadPath):
                    logger.debug(f"Deleting {_downloadPath}...")
                    os.remove(_downloadPath)
            except Exception as err:
                logger.critical(f"Cannot delete {_downloadPath}...")
                logger.critical(err)
                self.filesDownloaded = False
                return False

            _downloadCMD = f"curl --get --silent --insecure {_webPath} --output {_downloadPath} " + '-w "%{http_code}"'

            logger.info(f"=== Downloading {_webPath} ==========")
            _download = subprocess.Popen( args=_downloadCMD, 
                             stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell=True )

            (output, err) = _download.communicate()

            logger.info(output.decode("UTF-8"))

            if int(output.decode("UTF-8")) != 200:
                logger.critical(err.decode('utf-8'))
                return
        
        
        self.filesDownloaded = True
        logger.info("Files dowloaded!")


            
if __name__ == '__main__':
    downloader = Download(fileNames = ["countries.csv"])
    downloader.download_files()
    print(downloader.filesDownloaded)