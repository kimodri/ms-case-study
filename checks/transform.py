import pandas as pd
import json, os
import numpy as np

# Get the json file with sys
files = os.listdir('./files')
with open(f"files/{files[-1]}", 'r', encoding="utf-8") as f:
    data = json.load(f)

def convert(x):
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
        df[col] = df[col].apply(convert)
    
    # Add a source field from the most recent file
    df["source_file"] = files[-1]
    
    # Subset the dataframe
    df = df[fields]

    return df

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