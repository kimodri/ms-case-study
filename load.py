# use the blob
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os, sys, json
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError

# Get the file to load
if len(sys.argv) < 2:
    print("load.py requires the JSON file to load as an argument")
    sys.exit(1)
elif len(sys.argv) > 2:
    print("load.py accepts only one argument.")
else:
    blob_name = sys.argv[1]

# load the environment variable
load_dotenv()
string_connection = os.getenv("DATABASE_URI")
azure_blob_str_connection = os.getenv("AZURE_BLOB_CON")

# Connect to blob
blob_service_client = BlobServiceClient.from_connection_string(azure_blob_str_connection)
container_name = "files"
blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
source_name = blob_client.blob_name

# Download the data
try:
    downloaded_data = blob_client.download_blob().readall()
    data = json.loads(downloaded_data)
    
except ResourceNotFoundError:
    print("Blob does not exist.")
    sys.exit(1)
except Exception as e:
    print("Other error:", e)
    sys.exit(1)

def _convert(x):
    if isinstance(x, (list, dict)):
        if len(x) == 0:  # empty list or dict
            return None
        return json.dumps(x)
    if pd.isna(x):
        return None
    return x
    
def transform(df, *fields):
    """
        Transform DataFrame:
        - Converts lists/dicts to JSON strings
        - Keeps empty or null ones as None

        Parameters:
            df (pandas.DataFrame): The dataframe to transform.
            fields: 

        Returns:
            a transformed dataframe.
    """

    # Check for the fields, return if no fields is needed
    if (len(fields) < 1):
        return 
    
    # Check the type of the arbitrary arguments
    if not all(isinstance(field, str) for field in fields):
        raise TypeError("All arguments following the dataframe must be strings.")

    # Get the wanted fields
    fields = [field for field in fields]

    # Transform objects to JSON, handle empty ones as null
    for col in df.columns:
        df[col] = df[col].apply(_convert)
    
    # Add a source field from the most recent file
    df["source_file"] = source_name
    
    # Subset the dataframe
    df = df[fields]

    return df

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
    # Check if the JSON file has already loaded
    with engine.connect() as conn:

        source_name = df['source_file'].iloc[0]

        result = conn.execute(
            text(f"SELECT COUNT(*) FROM {table_name} WHERE source_file = :src"), {"src": source_name}
        )

        # check the number of rows
        count = result.scalar()

    if count > 0:
        print("JSON file was already loaded. Skipping loading...")
        return 

    df.to_sql(table_name, engine, if_exists="append", index=False)
    print(f"Successfully loaded: {table_name}")

# create dataframes to process

# module table
df_modules = pd.DataFrame(data.get('modules'))
df_modules = transform(
    df_modules,
    'title', 'summary', 'locale', 'levels', 'roles',
    'products', 'subjects', 'url', 'last_modified', 'source_file', 
)

# units table
df_units = pd.DataFrame(data.get('units'))
df_units = transform(
    df_units,
    'title', 'locale', 'duration_in_minutes',
    'last_modified', 'source_file', 
)

# learning paths
df_learning_paths = pd.DataFrame(data.get('learningPaths'))
df_learning_paths = transform(
    df_learning_paths,
    'title', 'summary', 'locale', 'levels',
    'products', 'subjects', 'source_file', 
)

# applied skills
df_applied_skills = pd.DataFrame(data.get('appliedSkills'))
df_applied_skills = transform(
    df_applied_skills,
    'title', 'summary', 'locale', 'levels', 'roles',
    'products', 'subjects', 'url', 'last_modified', 'source_file', 
)

# certifications
df_certifications = pd.DataFrame(data.get('certifications'))
df_certifications = transform(
    df_certifications,
    'title', 'subtitle', 'url', 'last_modified',
    'certification_type', 'exams', 'levels', 'roles', 'source_file', 
)

# mergedCertifications
df_merged_certifications = pd.DataFrame(data.get('mergedCertifications'))
df_merged_certifications = transform(
    df_merged_certifications,
    'title', 'summary', 'url', 'last_modified',
    'certification_type', 'products', 'levels', 'roles', 'subjects',
    'prerequisites', 'skills', 'providers', 'career_paths', 'source_file', 
)

# exams
df_exams = pd.DataFrame(data.get('exams'))
df_exams = transform(
    df_exams,
    'title', 'subtitle', 'url',
    'last_modified', 'levels', 'roles', 'products', 'providers', 'source_file', 
)

# courses
df_courses = pd.DataFrame(data.get('courses'))
df_courses = transform(
    df_courses,
    'title', 'summary', 'duration_in_hours',
    'url', 'last_modified', 'levels', 'roles', 'products', 'source_file', 
)

# levels
df_levels = pd.DataFrame(data.get('levels'))
df_levels = transform(df_levels, 'name', 'source_file')

# products
df_products = pd.DataFrame(data.get('products'))
df_products = transform(df_products, 'name', 'children', 'source_file')

# roles
df_roles = pd.DataFrame(data.get('roles'))
df_roles = transform(df_roles, 'name', 'source_file')

# subjects
df_subjects = pd.DataFrame(data.get('subjects'))
df_subjects = transform(df_subjects, 'name', 'children', 'source_file')


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

# connect to the database
try:
    conn_engine = create_engine(string_connection)
    print("connected to database")

    # load to the database
    for key, value in tables_dict.items():
        load(value, key, conn_engine)

except Exception as e: 
    print(f"Cannot establish connection :(\nError: {e}")