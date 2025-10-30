import pandas as pd
import os, sys, json
from log import logger
from utility import transform, check_duplicates
from dotenv import load_dotenv
from sqlalchemy import create_engine
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError

# Set timeout
CONNECTION_TIMEOUT_SECONDS = 90

# Get the file to load
if len(sys.argv) < 2:
    print("load.py requires the JSON file to load as an argument")
    sys.exit(1)
elif len(sys.argv) > 2:
    print("load.py accepts only one argument.")
else:
    blob_name = sys.argv[1]

# load the environment variables
load_dotenv()
string_connection = os.getenv("DATABASE_URI")
azure_blob_str_connection = os.getenv("AZURE_BLOB_CON")

# Connect to the database
try:
    logger.info("Connecting to the database ...")
    conn_engine = create_engine(string_connection)
    
    logger.info("Connected to the database ...")
except Exception as e: 
    print(f"Cannot establish connection :(\nError: {e}")
    sys.exit(1)

# check for duplicates
logger.info("Checking if file already exists ...")
check_duplicates(conn_engine, blob_name)

# Connect to blob and download the data
try:
    logger.info(f"Connecting to Azure Storage ...")
    blob_service_client = BlobServiceClient.from_connection_string(
        azure_blob_str_connection,
        connection_timeout=CONNECTION_TIMEOUT_SECONDS
    )
    container_name = "files"
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    source_name = blob_client.blob_name
    downloaded_data = blob_client.download_blob().readall()
    data = json.loads(downloaded_data)
    logger.info(f"Data from Azure has been downloaded successfully...") 
except ResourceNotFoundError:
    print("Blob does not exist.")
    sys.exit(1)
except Exception as e:
    print("Other error:", e)
    sys.exit(1)


def load(df, table_name, engine):
    """
        Loads a DataFrame into a SQL table if it hasn't been loaded yet.

        Parameters:
            df (pandas.DataFrame): The dataframe holding the table.
            table_name (str): The name of the SQL table to load into.
            engine: SQLAlchemy engine used for database connection.

        Returns:
            None
    """    
    df.to_sql(table_name, engine, if_exists="append", index=False)
    print(f"Successfully loaded: {table_name}")

# create dataframes to process
logger.info("Tables are being transformed ...")

# module table
df_modules = pd.DataFrame(data.get('modules'))
df_modules = transform(
    df_modules, source_name,
    'title', 'summary', 'locale', 'levels', 'roles',
    'products', 'subjects', 'url', 'last_modified', 'source_file', 
)

# units table
df_units = pd.DataFrame(data.get('units'))
df_units = transform(
    df_units, source_name,
    'title', 'locale', 'duration_in_minutes',
    'last_modified', 'source_file', 
)

# learning paths
df_learning_paths = pd.DataFrame(data.get('learningPaths'))
df_learning_paths = transform(
    df_learning_paths, source_name,
    'title', 'summary', 'locale', 'levels',
    'products', 'subjects', 'source_file', 
)

# applied skills
df_applied_skills = pd.DataFrame(data.get('appliedSkills'))
df_applied_skills = transform(
    df_applied_skills, source_name,
    'title', 'summary', 'locale', 'levels', 'roles',
    'products', 'subjects', 'url', 'last_modified', 'source_file', 
)

# certifications
df_certifications = pd.DataFrame(data.get('certifications'))
df_certifications = transform(
    df_certifications, source_name,
    'title', 'subtitle', 'url', 'last_modified',
    'certification_type', 'exams', 'levels', 'roles', 'source_file', 
)

# mergedCertifications
df_merged_certifications = pd.DataFrame(data.get('mergedCertifications'))
df_merged_certifications = transform(
    df_merged_certifications, source_name,
    'title', 'summary', 'url', 'last_modified',
    'certification_type', 'products', 'levels', 'roles', 'subjects',
    'prerequisites', 'skills', 'providers', 'career_paths', 'source_file', 
)

# exams
df_exams = pd.DataFrame(data.get('exams'))
df_exams = transform(
    df_exams, source_name,
    'title', 'subtitle', 'url',
    'last_modified', 'levels', 'roles', 'products', 'providers', 'source_file', 
)

# courses
df_courses = pd.DataFrame(data.get('courses'))
df_courses = transform(
    df_courses, source_name,
    'title', 'summary', 'duration_in_hours',
    'url', 'last_modified', 'levels', 'roles', 'products', 'source_file', 
)

# levels
df_levels = pd.DataFrame(data.get('levels'))
df_levels = transform(df_levels, source_name, 'name', 'source_file')

# products
df_products = pd.DataFrame(data.get('products'))
df_products = transform(df_products, source_name, 'name', 'children', 'source_file')

# roles
df_roles = pd.DataFrame(data.get('roles'))
df_roles = transform(df_roles, source_name, 'name', 'source_file')

# subjects
df_subjects = pd.DataFrame(data.get('subjects'))
df_subjects = transform(df_subjects, source_name, 'name', 'children', 'source_file')
logger.info("Tables have been transformed and now ready to be loaded ...")

tables_dict = {
    "modules": df_modules,
    "units": df_units,
    "learning_paths": df_learning_paths,
    "applied_skills": df_applied_skills,
    "certifications": df_certifications,
    "merged_certifications": df_merged_certifications,
    "exams": df_exams,
    "courses": df_courses,
    "levels": df_levels,
    "products": df_products,
    "roles": df_roles,
    "subjects": df_subjects
}

logger.info("Loading the tables ...")
for key, value in tables_dict.items():
    load(value, key, conn_engine)

