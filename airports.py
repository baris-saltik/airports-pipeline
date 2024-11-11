import logging.config, sys, os
import os, pathlib, flask, datetime, logging, time
from flask import Flask, render_template, redirect, url_for, session, request, send_from_directory, flash
from flask_wtf import FlaskForm


# Step 1 ################## Read main config and update logging config #################
# Set the logging.conf file first before other modules loads it.
from modules.main_config.main_config import Config
from modules.log_config.log_config import LoggingConf

configObj = Config()
configObj.update_main_conf()
configObj.set_defaults()
configObj.initialize_logging_conf()
configObj.update_logging_conf()
config = configObj.mainConfigDict

# Initialize logging
loggingConfig = LoggingConf().config
logging.config.dictConfig(loggingConfig)
logger = logging.getLogger('airports')


# Step 2 ################## Load the rest of the custom modules #################
# Load the custom modules which relies on logging configuration

from modules.form.form import LoginForm, DownloadForm, DatabaseForm, UploadForm, ControlForm, CreateScriptsForm
from modules.ddlh.ddlh import DDLH
from modules.download.download import Download
from modules.postgres.postgres import Postgres
from modules.csv_parser.parser import Parser
from modules.scripts.scripts import Script
from modules.upload.upload import Upload

# Step 3 ################## Initialize Flask app #################
logger.info(f"Airports loading...")

static_folder = os.path.join(pathlib.Path(__file__).resolve().absolute().parents[0], 'site', 'static')
template_folder = os.path.join(pathlib.Path(__file__).resolve().absolute().parents[0], 'site', 'templates')
definitions = ['catalogs', 'hive', 'iceberg', 'postgresql', 'federated', 'powerbi', 'views']

app = Flask(import_name = __name__, static_folder = static_folder, template_folder = template_folder)

# logger.info(__file__)

app.config['SECRET_KEY'] = "myVerySecretKey"
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0

# Cache controls
@app.before_request
def before_request():
    flask.session.permanent = True
    app.permanent_session_lifetime = datetime.timedelta(minutes=20)
    flask.session.modified = True

@app.after_request
def add_header(response):
    """
    Add headers to both force latest IE rendering engine or Chrome Frame,
    and to cache the rendered page for 0 minutes.
    """
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"

    return response

@app.route('/', methods = ['GET', 'POST'])
def index():

    # If there is a session, check if it is authenticated. If not redirect to log_in function.
    try:
        if session["authenticated"] == False:
            return redirect(url_for('login'))
    except Exception as err:
        return redirect(url_for('login'))


    ############## Initialize forms ################
    downloadForm = DownloadForm()
    databaseForm = DatabaseForm()
    uploadForm = UploadForm()
    controlForm = ControlForm()
    createScriptsForm = CreateScriptsForm()

    if request.method == 'GET':

        try:
            session["filesDownloaded"] = False
            session["tablesCreated"] = False
            session["filesUploaded"] = False
            session["scriptsCreated"] = False
            session['mviewPrivileges'] = config['script']['mviewPrivileges']
            session['operationFailed'] = False
        except Exception as err:
            return redirect(url_for('login'))
        
        return render_template('index.html', downloadForm = downloadForm, databaseForm = databaseForm, uploadForm = uploadForm, controlForm = controlForm, createScriptsForm = createScriptsForm)
    



    ############## Download Form ################
    if request.method == 'POST' and not session["filesDownloaded"] and downloadForm.validate_on_submit():
        session['operationFailed'] = False
        baseWebPath = downloadForm.baseWebPathField.data
        sourcesBasePath = downloadForm.sourcesBasePathField.data
        fileNames = config['download']['fileNames']
        # fileNames = ['countries.csv']
        
        logger.info(f"Downloading OurAirports data...")

        downloader = Download(baseWebPath = baseWebPath, sourcesBasePath = sourcesBasePath, fileNames = fileNames )
        downloader.download_files()
        session["filesDownloaded"] = downloader.filesDownloaded
        
        if not downloader.filesDownloaded:
            session['operationFailed'] = True
            #### Flash messages goe in here! ###
            flash("Download failed!") 
            flash("Check the values for correctness and connectivity")
            

        return render_template('index.html', downloadForm = downloadForm, databaseForm = databaseForm, uploadForm = uploadForm, controlForm = controlForm, createScriptsForm = createScriptsForm)

    ############## Database Form ################
    if request.method == 'POST' and not session["tablesCreated"] and databaseForm.validate_on_submit():
        session['operationFailed'] = False
        session['commitSize'] = databaseForm.commitSizeField.data
        session['dbHost'] = databaseForm.dbHostField.data
        session['dbPort'] = databaseForm.dbPortField.data
        session['dbname'] = databaseForm.dbnameField.data
        session['defaultDbname'] = databaseForm.defaultDbnameField.data
        session['schema'] = databaseForm.schemaField.data
        session['dbUsername'] = databaseForm.dbUsernameField.data
        session['dbPassword'] = databaseForm.dbPasswordField.data
        fileNames = config['download']['fileNames']
        baseWebPath = downloadForm.baseWebPathField.data
        sourcesBasePath = downloadForm.sourcesBasePathField.data
        
        database = Postgres(host = session['dbHost'], port = session['dbPort'],
                username = session['dbUsername'], password = session['dbPassword'], defaultDbname = session['defaultDbname'],
                dbname = session['dbname'], schema = session['schema'])
        _message = f"Creating {session['dbname']} database and {session['schema']} schema..."
        logger.info(_message)

        # Create schemas and tables
        try: 
            database.create_database()
            database.create_schema()

            for fileName in fileNames:

                filePath = os.path.join(sourcesBasePath, fileName)
                commitSize = int(session['commitSize'])

                # Trim .csv extension from filename to yield table name
                tableName = fileName.rstrip("csv").rstrip(".")

                print("\n", f"=== Creating and inserting rows into {tableName} table ==========")

                # Initialize csv parser object
                parser = Parser()

                # Crawl through the csv file, create (or recreate) the tables and insert records from the csv file
                logger.info(f"Inserting into {tableName}")
                parser.insert_rows(database = database, tableName = tableName, filePath = filePath, commitSize = commitSize)
            
            # time.sleep(3)
            session["tablesCreated"] = True
        
        except Exception as err:
            session["tablesCreated"] = False
            session['operationFailed'] = True
            #### Flash messages goe in here! ###
            flash("Tables creation failed!") 
            flash("Check the values for correctness and connectivity")

        return render_template('index.html', downloadForm = downloadForm, databaseForm = databaseForm, uploadForm = uploadForm, controlForm = controlForm, createScriptsForm = createScriptsForm)

    
    ############## Upload Form ################
    if request.method == 'POST' and session["tablesCreated"] and not session["filesUploaded"] and uploadForm.validate_on_submit():
        session['operationFailed'] = False
        session['key'] = uploadForm.keyField.data
        session['secret'] = uploadForm.secretField.data
        session['endpoint'] = uploadForm.endpointField.data
        session['bucketName'] = uploadForm.bucketNameField.data
        fileNames = config['download']['fileNames']

        _message = f"Uploading OurAirports data to {session['bucketName']} database on {session['endpoint']} S3 storage..."
        logger.info(_message)

        ### Upload config will go in here!
        try:
            uploader = Upload( key = session['key'], secret = session['secret'], endpoint = session['endpoint'], bucketName = session['bucketName'], fileNames = fileNames )
            uploader.create_bucket()
            uploader.upload_files()
            session["filesUploaded"] = True
        except Exception as err:
            logger.critical(err)
            session["filesUploaded"] = False
            session['operationFailed'] = True
            #### Flash messages goe in here! ###
            flash("Files upload failed!") 
            flash("Check the values for correctness and connectivity")

        # time.sleep(3)

        return render_template('index.html', downloadForm = downloadForm, databaseForm = databaseForm, uploadForm = uploadForm, controlForm = controlForm, createScriptsForm = createScriptsForm)
    


    ############## Scripts Form ################
    if request.method == 'POST' and not session["scriptsCreated"] and session["filesDownloaded"] and session["tablesCreated"] and session["filesUploaded"] and createScriptsForm.validate_on_submit():

        
        session['operationFailed'] = False
        logger.info("Creating DDAE scripts and setting materialized view privileges...")
        try:
            session['scriptsBasePath'] = createScriptsForm.scriptsBasePathField.data
            scriptCreator = Script(scriptsBasePath = session['scriptsBasePath'], 
                                   host = session['host'], port = session['port'], username = session['username'], password = session['password'],
                                   endpoint = session['endpoint'], bucketName = session['bucketName'], key = session['key'], secret = session['secret'],
                                   dbHost = session['dbHost'], dbPort = session['dbPort'], dbUsername = session['dbUsername'], dbPassword = session['dbPassword'], postgresqlDBName = session['dbname'], postgresqlSchema = session['schema']
                                   )

            scriptCreator.create_catalogs_script()
            scriptCreator.create_powerbi_script()
            scriptCreator.create_hive_script()
            scriptCreator.create_iceberg_script()
            scriptCreator.create_postgresql_script()
            scriptCreator.create_views_script()     
            scriptCreator.create_federated_queries_script()
            scriptCreator.pack_scripts()
            session["scriptsCreated"] = True

        except Exception as err:
            session["scriptsCreated"] = False
            session['operationFailed'] = False
            logger.critical(err)
            #### Flash messages goe in here! ###
            flash("Scripts creation failed!") 
            flash("Check the values for correctness")
           

        session['mviewPrivileges'] = createScriptsForm.mviewPrivilegesField.data

        print(f"Privileges: {session['mviewPrivileges']}")

        if session['mviewPrivileges'] and session["scriptsCreated"]:
            logger.info("Setting materialized view privileges...")
            #### Enter DDAE Privileges! ####

            # Authenticate
            ddlh = DDLH(host = session['host'], port = session['port'], username = session['username'], password = session['password'])

            try:
                ddlh.log_in()
                session['authenticated'] = ddlh.authenticated
                logger.info("Logged into DDAE.")

            except Exception as err:
                logger.error(err)
                logger.critical("Could not log in to DDAE. Quiting...")
                session["authenticated"] = False

            if session['authenticated']:

                try:
                    # Table Scan Reirect Privileges
                    ddlh.create_redirect_role()
                    ddlh.assign_redirect_grants()
                    ddlh.assign_redirect_role_to_service_user()

                    # Materialized View Privileges
                    ddlh.get_system_role_id()
                    ddlh.create_mview_role()
                    ddlh.assign_mview_grants()
                    ddlh.assign_mview_role_to_sytem_user()
                except Exception as err:
                    logger.error(err)
                    session['operationFailed'] = True
                    #### Flash messages goe in here! ###
                    flash("Roles and privileges creation failed!") 
                    flash("Check the values for correctness and connectivity")
            
            else:
                    logger.error("Could not connect to DDAE!")
                    session['operationFailed'] = True
                    #### Flash messages goe in here! ###
                    flash("Roles and privileges creation failed!") 
                    flash("Check the values for correctness and connectivity")



        return render_template('index.html', downloadForm = downloadForm, databaseForm = databaseForm, uploadForm = uploadForm, controlForm = controlForm, createScriptsForm = createScriptsForm)








    ############## Controls Form ################
    if request.method == 'POST' and session["filesDownloaded"] and session["tablesCreated"] and session["filesUploaded"] and session["scriptsCreated"] and controlForm.validate_on_submit():

        logger.info(f"Downloading scripts file...")

        app.config['UPLOAD_FOLDER'] = os.path.join(app.root_path, session['scriptsBasePath'])
        
        return send_from_directory(directory = app.config['UPLOAD_FOLDER'], path = config['script']['packedScriptsFileName'])

        # return render_template('index.html', downloadForm = downloadForm, databaseForm = databaseForm, uploadForm = uploadForm, controlForm = controlForm, createScriptsForm = createScriptsForm)

    return render_template('index.html', downloadForm = downloadForm, databaseForm = databaseForm, uploadForm = uploadForm, controlForm = controlForm, createScriptsForm = createScriptsForm)


@app.errorhandler(404)
def page_not_found(error):
    return render_template('page_not_found.html'), 404



@app.route('/scripts/<item>', methods = ['GET'])
def scripts(item):

    renderedItem = "Not found"

    try:
        authenticated = session["authenticated"]
    except Exception as err:
        authenticated = False

    try:
        scriptsCreated = session["scriptsCreated"]
    except Exception as err:
        scriptsCreated = False

    try:
        logger.info(f"Rendering {item} script...")
        if scriptsCreated and item in definitions:
            scriptCreator = Script(scriptsBasePath = session['scriptsBasePath'], 
                                    host = session['host'], port = session['port'], username = session['username'], password = session['password'],
                                    endpoint = session['endpoint'], bucketName = session['bucketName'], key = session['key'], secret = session['secret'],
                                    dbHost = session['dbHost'], dbPort = session['dbPort'], dbUsername = session['dbUsername'], dbPassword = session['dbPassword'], postgresqlDBName = session['dbname'], postgresqlSchema = session['schema']
                                    )

            if item.lower() == 'catalogs': 
                scriptCreator.create_catalogs_script()
                renderedItem = scriptCreator.catalogs

            if item.lower() == 'powerbi':
                scriptCreator.create_powerbi_script()
                renderedItem = scriptCreator.powerbi

            if item.lower() == 'hive': 
                scriptCreator.create_hive_script()
                renderedItem = scriptCreator.hive

            if item.lower() == 'iceberg':
                scriptCreator.create_iceberg_script()
                renderedItem = scriptCreator.iceberg

            if item.lower() == 'postgresql':
                scriptCreator.create_postgresql_script()
                renderedItem = scriptCreator.postgresql

            if item.lower() == 'views':
                scriptCreator.create_views_script()
                renderedItem = scriptCreator.views

            if item.lower() == 'federated':
                scriptCreator.create_federated_queries_script()
                renderedItem = scriptCreator.federated

    except Exception as err:
        logger.critical(err)

    return render_template('script.html', authenticated = authenticated, scriptsCreated = scriptsCreated, item = item, definitions = definitions, renderedItem = renderedItem)


@app.route('/login', methods = ['GET', 'POST'])
def login():

    # If "Log Out" button redirects browser to /login again, try to invert session["authenticated"] value.
    try:
        session["authenticated"] = False
    except Exception as err:
        pass

    # session["authenticated"] = False

    loginForm = LoginForm()

    # If form is submitted...
    if request.method == 'POST' and loginForm.validate_on_submit():
        session['username'] = loginForm.usernameField.data
        session['password'] = loginForm.passwordField.data
        session['host'] = loginForm.hostField.data
        session['port'] = loginForm.portField.data

        # Authenticate
        ddlh = DDLH(host = session['host'], port = session['port'], username = session['username'], password = session['password'])

        try:
            ddlh.log_in()
            session['authenticated'] = ddlh.authenticated

        except Exception as err:
            logger.error(err)
            session["authenticated"] = False

        #### Bypass Authentication!!!! ########
        session["authenticated"] = True

        if session["authenticated"]:
            return redirect(url_for('index'))
        else:
            logger.error("Login failed! Check the address and the port or the user credentials.")

    
    return render_template('login.html', loginForm = loginForm)

if __name__ == '__main__':
    app.run( host = '0.0.0.0', port = 5000, debug = False, ssl_context = 'adhoc' )