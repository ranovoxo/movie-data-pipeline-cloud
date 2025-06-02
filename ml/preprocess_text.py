import re
import logging
import os
import pandas as pd
from logger import *
from airflow.models import Variable
from db.db_connector import get_engine


def clean_text(text):
    if not isinstance(text, str):
        return ""
    
    # convert text to lowercase
    text = text.lower()

    # remove punctuation and digits (keeping only letters and spaces)
    text = re.sub(r'[^a-z\s]', '', text)
    # remove extra spaces
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def load_and_clean_movies():
    engine = get_engine()
    df = pd.read_sql("SELECT * FROM movies_silver", engine)

    table_name = "cleaned_overview_text"

    # apply cleaning function to the overview column in the data frame
    df['cleaned_overview'] = df['overview'].apply(clean_text)
    
    # Overwrite or create the staging table
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"✅ Data saved to table {table_name}")
    print(df['cleaned_overview'])
    return df


def preprocess_text():
    # start preprocess to clean overview column text data
    df = load_and_clean_movies()  