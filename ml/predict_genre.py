import os
from urllib.parse import urlparse

import boto3
import joblib
import numpy as np
import pandas as pd
from db.db_connector import get_engine

SOURCE_TABLE = "cleaned_overview_text"
LOCAL_ARTIFACT_DIR = "/tmp/ml_artifacts"

def load_data():
    engine = get_engine()
    df = pd.read_sql(f"SELECT title, genres, cleaned_overview FROM {SOURCE_TABLE}", engine)
    return df


def load_artifacts(artifact_uris):
    os.makedirs(LOCAL_ARTIFACT_DIR, exist_ok=True)
    s3 = boto3.client("s3")
    artifacts = {}
    for name, uri in artifact_uris.items():
        parsed = urlparse(uri)
        bucket = parsed.netloc
        key = parsed.path.lstrip('/')
        local_path = os.path.join(LOCAL_ARTIFACT_DIR, os.path.basename(key))
        s3.download_file(bucket, key, local_path)
        artifacts[name] = joblib.load(local_path)
        print(f"Downloaded {name} from {uri} to {local_path}")
    return artifacts['model'], artifacts['vectorizer'], artifacts['label_binarizer']


def predict_genres(df, clf, vectorizer, mlb):
    predictions = []
    results = []
    total = len(df)
    count = 1

    print("Sample input:", df.iloc[0]['cleaned_overview'])
    print("Vectorized shape:", vectorizer.transform([df.iloc[0]['cleaned_overview']]).shape)
    print("Predicted labels:", clf.predict(vectorizer.transform([df.iloc[0]['cleaned_overview']]))[0])

    for i, row in df.iterrows():
        title = row['title']
        overview_text = row['cleaned_overview']
        print(overview_text)
        actual_genres = set(g.strip() for g in row['genres'].split(',') if g.strip())  # assumes genres is a list or tuple

        # predict
        X = vectorizer.transform([overview_text])
        y_pred = clf.predict(X)
        print("Predicted y:", y_pred)

        # inverse transform using MultiLabelBinarizer
        if hasattr(y_pred, 'toarray'):  # e.g., sparse matrix
            y_pred = y_pred.toarray()

        predicted_genres = mlb.inverse_transform(y_pred)[0] if y_pred.any() else ()
        predicted_genres_set = set(predicted_genres)

        # check if prediction matches actual genres
        passed = predicted_genres_set == actual_genres

        # append result to list to save to dataframe
        results.append({
            'title': title,
            'actual_genres': list(actual_genres),
            'predicted_genres': list(predicted_genres),
            'pass': passed
        })

        # airflow logs to show predictions made out of number of rows in dataframe
        print(f"Prediction {count} out of {total} done.")
        count += 1

    return pd.DataFrame(results)


def save_predictions(df, table_name='ml_genre_predictions'):
    engine = get_engine()
    df.to_sql(table_name, engine, if_exists='replace', index=False)


def start_genre_predictions(ti=None, **kwargs):
    artifact_uris = {}
    if ti:
        artifact_uris = ti.xcom_pull(task_ids='train_genre_ml', key='artifacts') or {}
    if not artifact_uris:
        raise ValueError("Artifact URIs not found in XCom")
    clf, vectorizer, mlb = load_artifacts(artifact_uris)
    df = load_data()
    predicted_genres = predict_genres(df, clf, vectorizer, mlb)
    save_predictions(predicted_genres)
    print("Predicted genres:", predicted_genres)


if __name__ == "__main__":
    start_genre_predictions()
