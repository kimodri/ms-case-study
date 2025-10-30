import pandas as pd
import json, sys
from sqlalchemy import text, inspect
from log import logger

def _convert(x):
    if isinstance(x, (list, dict)):
        if len(x) == 0:  # empty list or dict
            return None
        return json.dumps(x)
    if pd.isna(x):
        return None
    return x
    
def transform(df, source_name, *fields):
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

def check_duplicates (engine, source):
    with engine.connect() as con:
        inspector = inspect(engine)
        table_names = inspector.get_table_names()
        sum = 0
        for table_name in table_names:
            count = 0
            result = con.execute(
                text(f"SELECT COUNT(*) FROM {table_name} WHERE source_file = :src"), {"src": source}
            )
            count = result.scalar()
            sum += count
        if sum > 0:
            logger.error("This file already exists in the database. Exiting ...")
            sys.exit(1)