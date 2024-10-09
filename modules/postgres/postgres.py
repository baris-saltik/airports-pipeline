from sqlalchemy import create_engine, text, insert, MetaData, Table, Column, Integer, VARCHAR, REAL
import psycopg, re, sys, os, pathlib, logging, logging.config

# Add "modules" to sys.path
sys.path.append(os.path.join(pathlib.Path(__file__).resolve().absolute().parents[2], "modules"))

from log_config.log_config import LoggingConf

loggingConfig = LoggingConf()
logging.config.dictConfig(loggingConfig.config)
logger = logging.getLogger(__name__)

logger.info("Initializing postgres module...")


host = ''
port = '5432'
username = ''
password = ''
defaultDbname = 'starburst'
dbname = 'airportsdb'
schema = 'airports'
tableName = 'airports'
# url = f"postgresql+psycopg://{username}:{password}@{host}:{port}/{dbname}" 
# fieldNames = ['id', 'ident', 'type', 'name', 'latitude_deg', 'longitude_deg', 'elevation_ft', 'continent', 'iso_country', 'iso_region', 'municipality', 'scheduled_service', 'gps_code', 'iata_code', 'local_code', 'home_link', 'wikipedia_link', 'keywords']

selectAllStatement = f"select * from {schema}.{tableName}"
insertStatement = f"insert into {schema}.{tableName} VALUES (3,'Portland'),(4, 'Johannesburg')"
values = None

class Postgres(object):

    def __init__(self, host = host, port = port,
                 username = username, password = password, defaultDbname = defaultDbname,
                 dbname = dbname, schema = schema, insertStatement = insertStatement):
        
        self.url = f"postgresql+psycopg://{username}:{password}@{host}:{port}/{dbname}"
        self.engine = create_engine(url = self.url)

        # Default database connection is used to create the main database.
        self.defaultUrl = f"postgresql+psycopg://{username}:{password}@{host}:{port}/{defaultDbname}" 
        self.defaultEngine = create_engine(url = self.defaultUrl)

        self.selectAllStatement = f"select * from {schema}.{tableName}"
        self.insertStatement = insertStatement
        
        self.metadata = MetaData()
        self.schema = schema

        self.dbname = dbname
        self.defaultDbname = defaultDbname

    def create_database(self):

        dbExists = True

        with self.defaultEngine.connect() as connection:

            # This sets the 'AUTOCOMMIT' isolation level on the _connection.
            # Without this connection stays in the transaction mode, and Postgresql does not accept DATABASE CREATE in a transaction.
            # When the _connection is returned to the connection pool, 'AUTOCOMMIT' mode is reverted.
            connection = connection.execution_options(isolation_level='AUTOCOMMIT')

            # Check if the main database already exists
            result = connection.execute(text(f"SELECT datname FROM pg_database WHERE datname = '{self.dbname}'"))

            if not result.all():
                dbExists = False

            if not dbExists:
                logger.info(f"Creating database {self.dbname}...")
                connection.execute(text(f"CREATE DATABASE {self.dbname}"))
                logger.info(f"Database {self.dbname} has been created.")
            else:
                logger.info(f"Database {self.dbname} already exists.")

    def create_schema(self):
        
        logger.info(f"Creating schema {self.schema}...")
        with self.engine.connect() as connection:
            connection = connection.execution_options(isolation_level='AUTOCOMMIT')
            connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.schema}"))
        logger.info("Schema created.")


    def insert_sql(self):
        with self.engine.connect() as _connection:
            _connection.execute(text(self.insertStatement))
            _connection.commit()

    def select_all_text(self):
        with self.engine.connect() as _connection:
            with _connection.execution_options(yield_per=100).execute(text(self.selectAllStatement)) as _result:
                for _partition in _result.mappings().partitions():
                    for _row in _partition:
                        logger.debug(_row["aid"], _row["name"])

    def create_table(self, engine = None, schema = None, tableName = None, fieldNames = None):

        if not tableName or not fieldNames:
            sys.exit("Error: Table name and/or field names for the table were not supplied. Quiting...")
        
        if engine: self.engine = engine
        if schema: self.schema = schema

        columns = []
        idFound = False

        if fieldNames:
            for fieldName in fieldNames:
                columns.append(fieldName)
                if fieldName == "id":
                    idFound = True

        if idFound:
            table = Table(tableName, self.metadata, 
                        Column("id", Integer, primary_key=True), 
                        *[ Column(cName, VARCHAR) for cName in columns if cName != "id" ],
                        schema=self.schema)
        else:
            table = Table(tableName, self.metadata, 
                        *[Column(cName, VARCHAR) for cName in columns],
                        schema=self.schema)

        # Drop the table if exists
        try:
            table.drop(self.engine)
            logger.info(f"Table {tableName} dropped")
        except Exception as err:
            logger.info(f"Table {tableName} either did not exist or could not be dropped!")

        # Create the table
        try:
            table.create(self.engine)
        except Exception as err:
            logger.error(err)
            logger.error(f"Table {tableName} could not be created!")
            sys.exit()
        
        logger.info(f"Table {tableName} has been created!")
    
    def insert(self, tableName = None, table = None, values = None):

        if not values:
            logger.warning("Nothing to insert!")
            sys.exit()

        if not tableName:
            logger.warning(f"{tableName} is not provided! Skipping...")

        try:
            table = self.metadata.tables[f"{self.schema}.{tableName}"]
        except Exception as err:
            logger.info(f"Table {tableName} does not exist!")
            sys.exit(f"Table {tableName} does not exist!")

        with self.engine.connect() as connection:
            result = connection.execute(
                insert(table), 
                values
                )
            connection.commit()


if __name__ == '__main__':

    database = Postgres()
    database.create_database()
    # database.select_all_text()
    database.create_table()
    database.insert()