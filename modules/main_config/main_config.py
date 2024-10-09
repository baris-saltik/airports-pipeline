import os, pathlib, sys
from yaml import load, dump, Loader, Dumper

modulesPath = sys.path.append(os.path.join(pathlib.Path(__file__).resolve().parents[2], "modules"))

from log_config.log_config import LoggingConf 

configBasePath = os.path.join(pathlib.Path(__file__).resolve().absolute().parents[2], 'config')
configPath = os.path.join(pathlib.Path(__file__).resolve().absolute().parents[2], 'config', 'main.yaml')
loggingConfigPath = os.path.join(pathlib.Path(__file__).resolve().absolute().parents[2], 'config', 'logging.yaml')

class Config(object):

    def __init__(self, configPath = configPath):

        self.configPath = configPath
        self.mainConfigDict = {}

        if not os.path.exists(configBasePath):
            os.makedirs(configBasePath)
        
        if not os.path.isfile(configPath):
            self.set_defaults()
            self.update_main_conf()     
        else:
            with open(configPath, "r") as configFile:
                self.mainConfigDict = load(stream = configFile, Loader = Loader)
            self.set_defaults()

    def set_defaults(self):

        ### Defaults
        # logging
        logging = {
            'level': 'DEBUG',
            'maxBytes': 10000,
            'backupCount': 5
        }

        if self.mainConfigDict.get('logging'):
            for k, v in logging.items():
                self.mainConfigDict['logging'].setdefault(k,v)
        else:
            self.mainConfigDict['logging'] = logging

        # ddlh
        ddlhDefaults = { 
            'host': '',
            'port':  '443',
            'username': '',
            'password': '',
            'systemRole': 'sysadmin',
            'serviceUser': 'dv-sev-svc',
            'redirectRole': 'redirect_role',
            'mviewRole': 'mview_role',
            'hiveSchema': 'airports',
            'icebergSchema': 'airports_products',
            'icebergCatalog': 'iceberg',
            'hiveCatalog': 'hive',
            'postgresqlCatalog': 'postgresext',
            'tableScanRedirectionSourceTable': 'runways'
        }

        if self.mainConfigDict.get('ddlh'):
            for k, v in ddlhDefaults.items():
                self.mainConfigDict['ddlh'].setdefault(k,v)
        else:
            self.mainConfigDict['ddlh'] = ddlhDefaults

        # database
        databaseDefaults = {
            'dbHost': '<db host ip>',
            'dbPort':'5432',
            'dbUsername': '',
            'dbPassword': '',
            'defaultDbname': 'starburst',
            'dbname': 'airportsdb',
            'schema': 'airports',
            'commitSize': 1000
        }

        if self.mainConfigDict.get('database'):
            for k, v in databaseDefaults.items():
                self.mainConfigDict['database'].setdefault(k,v)
        else:
            self.mainConfigDict['database'] = databaseDefaults

        # download
        downloadDefaults = {
            'baseWebPath': "https://davidmegginson.github.io/ourairports-data/",
            'fileNames': ["airports.csv", "airport-frequencies.csv", "runways.csv", "navaids.csv", "countries.csv", "regions.csv"],
            'sourcesBasePath': 'sources'
        }

        if self.mainConfigDict.get('download'):
            for k, v in downloadDefaults.items():
                self.mainConfigDict['download'].setdefault(k,v)
        else:
            self.mainConfigDict['download'] = downloadDefaults

        # upload
        uploadDefaults = {
            'endpoint':'http://<object-storage-ip>:9020',
            'key': '<key>',
            'secret':'<secret>',
            'bucketName': 'airports'
        }

        if self.mainConfigDict.get('upload'):
            for k, v in uploadDefaults.items():
                self.mainConfigDict['upload'].setdefault(k,v)
        else:
            self.mainConfigDict['upload'] = uploadDefaults

        # script
        scriptDefaults = {
            'scriptsBasePath': 'scripts',
            'mviewPrivileges': 'True',
            'packedScriptsFileName': 'scripts.zip'
        }

        if self.mainConfigDict.get('script'):
            for k, v in scriptDefaults.items():
                self.mainConfigDict['script'].setdefault(k,v)
        else:
            self.mainConfigDict['script'] = scriptDefaults
    
    def update_main_conf(self):
        with open(self.configPath, "w") as configFile:
            dump(stream = configFile, data = self.mainConfigDict, Dumper=Dumper)

    def initialize_logging_conf(self):
        loggingConfigObj = LoggingConf()
        loggingConfigObj.create_log_directories()
        loggingConfigObj.create_logging_conf( dynamicPartDict = self.mainConfigDict['logging'])
    
    def update_logging_conf(self, loggingConfigPath = loggingConfigPath):

        # Read existing logging config file
        with open(loggingConfigPath, "r") as loggingConfigFile:
            loggingConfig = load(loggingConfigFile, Loader = Loader)
        
        # Update logging config
        for id in loggingConfig['handlers'].keys():
            if id != 'stream':
                loggingConfig['handlers'][id]["maxBytes"] = self.mainConfigDict['logging']['maxBytes']
                loggingConfig['handlers'][id]["backupCount"]  = self.mainConfigDict['logging']['backupCount']

        for id in loggingConfig['loggers'].keys():
            loggingConfig['loggers'][id]['level'] = self.mainConfigDict['logging']['level']

        # loggingConfig['root']['level'] = self.mainConfigDict['logging']['level']

        # Write the updated config to the disk
        with open(loggingConfigPath, "w") as loggingConfigFile:
            loggingConfigFile.write('# DO NOT edit this file directly. Configure parameters under "logging" key in "main.yaml" file instead\n')
            loggingConfigFile.write(dump(loggingConfig, Dumper = Dumper, default_flow_style = False))


if __name__ == '__main__':
    configObj = Config()
    configObj.update_main_conf()
    configObj.set_defaults()
    configObj.initialize_logging_conf()
    configObj.update_logging_conf()
    

