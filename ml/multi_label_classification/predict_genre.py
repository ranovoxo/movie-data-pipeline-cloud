import joblib
import numpy as np
import pandas as pd
from db.db_connector import get_engine

SOURCE_TABLE = "cleaned_overview_text"

# load saved artifacts
clf = joblib.load('ml/multi_label_classification/genre_model.joblib')
vectorizer = joblib.load('ml/multi_label_classification/vectorizer.joblib')
mlb = joblib.load('ml/multi_label_classification/label_binarizer.joblib')

def load_data():
    engine = get_engine()
    df = pd.read_sql(f"SELECT title, genres, cleaned_overview FROM {SOURCE_TABLE}", engine)
    return df

def predict_genres(df):
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
        actual_genres = set(g.strip() for g in row['genres'].split(',') if g.strip()) # assumes genres is a list or tuple

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


def start_genre_predictions():
    df = load_data()
    predicted_genres = predict_genres(df)
    save_predictions(predicted_genres)
    print("Predicted genres:", predicted_genres)