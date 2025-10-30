import pandas as pd
import os, sys, json
from dotenv import load_dotenv
from sqlalchemy import create_engine
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError
from src.utility import transform, check_duplicates, logger

# Set timeout
CONNECTION_TIMEOUT_SECONDS = 90

# load the environment variables
load_dotenv()
string_connection = os.getenv("DATABASE_URI")
azure_blob_str_connection = os.getenv("AZURE_BLOB_CON")
container_name = "files"

def load(df, table_name, engine):
    """Loads a DataFrame into a SQL table"""    
    df.to_sql(table_name, engine, if_exists="append", index=False)
    logger.info(f"Successfully loaded: {table_name}")

def process_blob(blob_name=None, blob_content=None, blob_name_from_trigger=None):
    """Main load process. Either provide a filename (local/manual) or blob_content (Azure Function)"""
    try:
        logger.info("Connecting to the database ...")
        conn_engine = create_engine(string_connection)
        logger.info("Connected to the database ...")
    except Exception as e: 
        logger.error(f"Cannot establish connection :(\nError: {e}")
        sys.exit(1)

    # Check duplicates if blob_name is given
    if blob_name:
        logger.info("Checking if file already exists ...")
        check_duplicates(conn_engine, blob_name)
    
    if blob_content:  # which means process_blob was called by a trigger
        # Connect to blob and download the data
        data = json.loads(blob_content.decode("utf-8"))
        source_name = blob_name_from_trigger
        logger.info(f"Data from Azure has been downloaded successfully from trigger: {source_name}") 

    elif blob_name:
        try:
            logger.info(f"Connecting to Azure Storage to download {blob_name}...")
            blob_service_client = BlobServiceClient.from_connection_string(
                azure_blob_str_connection, connection_timeout=CONNECTION_TIMEOUT_SECONDS
            )
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
            downloaded_data = blob_client.download_blob().readall()
            
            data = json.loads(downloaded_data)
            source_name = blob_client.blob_name
            logger.info(f"Data from Azure has been downloaded successfully: {source_name}") 

        except ResourceNotFoundError:
            print("Blob does not exist.")
            sys.exit(1)
        except Exception as e:
            print("Other error:", e)
            sys.exit(1)
    else:
        logger.error("No blob_name or blob_content provided.")
    
    # Transform tables
    logger.info("Tables are being transformed ...")
    tables_dict = {}

    # module table
    df_modules = pd.DataFrame(data.get('modules'))
    tables_dict['modules'] = transform(
        df_modules, source_name,
        'title', 'summary', 'locale', 'levels', 'roles',
        'products', 'subjects', 'url', 'last_modified', 'source_file', 
    )

    # units table
    df_units = pd.DataFrame(data.get('units'))
    tables_dict['units'] = transform(
        df_units, source_name,
        'title', 'locale', 'duration_in_minutes',
        'last_modified', 'source_file', 
    )

    # learning paths
    df_learning_paths = pd.DataFrame(data.get('learningPaths'))
    tables_dict['learning_paths'] = transform(
        df_learning_paths, source_name,
        'title', 'summary', 'locale', 'levels',
        'products', 'subjects', 'source_file', 
    )

    # applied skills
    df_applied_skills = pd.DataFrame(data.get('appliedSkills'))
    tables_dict['df_applied_skills'] = transform(
        df_applied_skills, source_name,
        'title', 'summary', 'locale', 'levels', 'roles',
        'products', 'subjects', 'url', 'last_modified', 'source_file', 
    )

    # certifications
    df_certifications = pd.DataFrame(data.get('certifications'))
    tables_dict['certifications'] = transform(
        df_certifications, source_name,
        'title', 'subtitle', 'url', 'last_modified',
        'certification_type', 'exams', 'levels', 'roles', 'source_file', 
    )

    # mergedCertifications
    df_merged_certifications = pd.DataFrame(data.get('mergedCertifications'))
    tables_dict['merged_certifications'] = transform(
        df_merged_certifications, source_name,
        'title', 'summary', 'url', 'last_modified',
        'certification_type', 'products', 'levels', 'roles', 'subjects',
        'prerequisites', 'skills', 'providers', 'career_paths', 'source_file', 
    )

    # exams
    df_exams = pd.DataFrame(data.get('exams'))
    tables_dict['exams'] = transform(
        df_exams, source_name,
        'title', 'subtitle', 'url',
        'last_modified', 'levels', 'roles', 'products', 'providers', 'source_file', 
    )

    # courses
    df_courses = pd.DataFrame(data.get('courses'))
    tables_dict['courses'] = transform(
        df_courses, source_name,
        'title', 'summary', 'duration_in_hours',
        'url', 'last_modified', 'levels', 'roles', 'products', 'source_file', 
    )

    # levels
    df_levels = pd.DataFrame(data.get('levels'))
    tables_dict['levels'] = transform(df_levels, source_name, 'name', 'source_file')

    # products
    df_products = pd.DataFrame(data.get('products'))
    tables_dict['products'] = transform(df_products, source_name, 'name', 'children', 'source_file')

    # roles
    df_roles = pd.DataFrame(data.get('roles'))
    tables_dict['roles'] = transform(df_roles, source_name, 'name', 'source_file')

    # subjects
    df_subjects = pd.DataFrame(data.get('subjects'))
    tables_dict['subjects'] = transform(df_subjects, source_name, 'name', 'children', 'source_file')
    logger.info("Tables have been transformed and now ready to be loaded ...")

    logger.info("Loading tables into the database ...")
    for table_name, df in tables_dict.items():
        load(df, table_name, conn_engine)
    logger.info("Load process complete!")

# Get the file to load
if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("Please provide a JSON filename to load")
        sys.exit(1)
    blob_name_arg = sys.argv[1]
    process_blob(blob_name=blob_name_arg)



