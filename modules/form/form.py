import logging.config
import os, pathlib, sys, logging
from flask_wtf import FlaskForm
from wtforms.fields import StringField, PasswordField, IntegerField, SubmitField, BooleanField
from wtforms.validators import DataRequired, IPAddress

modulesPath = os.path.join(pathlib.Path(__file__).resolve().parents[2], "modules")
sys.path.append(modulesPath)

from modules.main_config.main_config import Config
from modules.log_config.log_config import LoggingConf

loggingConfig = LoggingConf().config
logging.config.dictConfig(config=loggingConfig)
logger = logging.getLogger(__name__)

logger.info("Initializing forms module...")

class LoginForm(FlaskForm):

    config = Config().mainConfigDict

    ddlhDefaultHost = config['ddlh']['host']
    ddlhDefaultPort = config['ddlh']['port']

    usernameField = StringField(validators=[DataRequired()])
    passwordField = PasswordField(validators=[DataRequired()])
    hostField = StringField(validators=[DataRequired()], default = ddlhDefaultHost)
    portField = StringField(validators=[DataRequired()], default = ddlhDefaultPort)
    submitField = SubmitField(label="Log in")

class DownloadForm(FlaskForm):

    config = Config().mainConfigDict

    baseWebPath = config['download']['baseWebPath']
    sourcesBasePath = config['download']['sourcesBasePath']
    fileNames = config['download']['fileNames']

    baseWebPathField = StringField(label = "Source path", validators=[DataRequired()], default = baseWebPath)
    sourcesBasePathField = StringField(label = "Download path", validators=[DataRequired()], default = sourcesBasePath)
    submitField = SubmitField(label = "Download")

class DatabaseForm(FlaskForm):

    config = Config().mainConfigDict

    commitSize = config['database']['commitSize']
    dbHost = config['database']['dbHost']
    dbPort = config['database']['dbPort']
    dbUsername = config['database']['dbUsername']
    dbPassword = config['database']['dbPassword']
    dbname = config['database']['dbname']
    defaultDbname = config['database']['defaultDbname']
    schema = config['database']['schema']

    commitSizeField = StringField(label = "Commit size", validators=[DataRequired()], default = commitSize)
    dbHostField = StringField(label = "Host", validators=[DataRequired()], default = dbHost)
    dbPortField = StringField(label = "Port", validators=[DataRequired()], default = dbPort)
    dbnameField = StringField(label = "Database", validators=[DataRequired()], default = dbname)
    defaultDbnameField = StringField(label = "Default Database", validators=[DataRequired()], default = defaultDbname)
    schemaField = StringField(label = "Schema", validators=[DataRequired()], default = schema)
    dbUsernameField = StringField(label = "Username", validators=[DataRequired()], default = dbUsername)
    dbPasswordField = StringField(label = "Password", validators=[DataRequired()], default = dbPassword)
    submitField = SubmitField(label = "Create")

class UploadForm(FlaskForm):

    config = Config().mainConfigDict

    key = config['upload']['key']
    secret = config['upload']['secret']
    endpoint = config['upload']['endpoint']
    bucketName = config['upload']['bucketName']

    keyField = StringField(label = "Key", validators = [DataRequired()], default = key)
    secretField = StringField(label = "Secret", validators = [DataRequired()], default = secret)
    endpointField = StringField(label = "Endpoint", validators = [DataRequired()], default = endpoint)
    bucketNameField = StringField(label = "Bucket", validators = [DataRequired()], default = bucketName)
    submitField = SubmitField(label = "Upload")

class ControlForm(FlaskForm):
    # resetField = SubmitField(label = "Reset")
    downloadScriptsField = SubmitField(label = "Download Scripts")

class CreateScriptsForm(FlaskForm):
    config = Config().mainConfigDict

    scriptsBasePath = config['script']['scriptsBasePath']
    mviewPrivileges = config['script']['mviewPrivileges']

    scriptsBasePathField = StringField(label = 'Scripts Path', validators = [DataRequired()], default = scriptsBasePath)
    mviewPrivilegesField = BooleanField(label = "Set Materialized View Privileges")

    submitField = SubmitField(label = "Create Scripts")


