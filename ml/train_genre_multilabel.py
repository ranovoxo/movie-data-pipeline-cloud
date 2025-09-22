import os
from datetime import datetime
import boto3
import joblib
import pandas as pd
from airflow.models import Variable
from sklearn.preprocessing import MultiLabelBinarizer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.multiclass import OneVsRestClassifier
from db.db_connector import get_engine
from iterstrat.ml_stratifiers import MultilabelStratifiedShuffleSplit
from sklearn.metrics import classification_report


TABLE_NAME = 'cleaned_overview_text'
S3_BUCKET = Variable.get('S3_BUCKET_NAME')
#os.getenv("S3_BUCKET_NAME", "your-bucket-name")
LOCAL_ARTIFACT_DIR = "/tmp/ml_artifacts"

def get_data():
    engine = get_engine()
    df = pd.read_sql(f"SELECT * FROM {TABLE_NAME}", engine)
    return df
    
def train_data(df):
    # preprocess genres column into list
    df['genre_list'] = df['genres'].fillna("").apply(lambda x: [g.strip() for g in x.split(',')])
    print(df['genre_list'])

    # binarize labels
    mlb = MultiLabelBinarizer()
    y = mlb.fit_transform(df['genre_list'])

    # vectorize text
    vectorizer = TfidfVectorizer(max_features=5000)
    X = vectorizer.fit_transform(df['cleaned_overview'])

    # training split for multi label data
    msss = MultilabelStratifiedShuffleSplit(n_splits=1, test_size=0.2, random_state=42)
    for train_index, test_index in msss.split(X, y):
        X_train, X_test = X[train_index], X[test_index]
        y_train, y_test = y[train_index], y[test_index]

    # train model
    clf = OneVsRestClassifier(RandomForestClassifier(n_estimators=100))
    clf.fit(X_train, y_train)

    y_train = clf.predict(X_train)
    print(classification_report(y_train, y_train, target_names=mlb.classes_))


    # save artifacts for inference for next task "predict_genere"
    os.makedirs(LOCAL_ARTIFACT_DIR, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H%M%S")
    s3_prefix = f"movie-genre-classifier/v={timestamp}/"
    s3 = boto3.client("s3")

    artifacts = {
        'model': (clf, 'genre_model.joblib'),
        'vectorizer': (vectorizer, 'vectorizer.joblib'),
        'label_binarizer': (mlb, 'label_binarizer.joblib'),
    }
    artifact_uris = {}

    for name, (obj, filename) in artifacts.items():
        local_path = os.path.join(LOCAL_ARTIFACT_DIR, filename)
        joblib.dump(obj, local_path)
        print(f"Training complete and {filename} saved locally at {local_path}.")
        s3_key = s3_prefix + filename
        s3.upload_file(local_path, S3_BUCKET, s3_key)
        uri = f"s3://{S3_BUCKET}/{s3_key}"
        print(f"Uploaded {filename} to {uri}")
        artifact_uris[name] = uri

    return artifact_uris

def start_training(ti=None, **kwargs):
    df = get_data()
    artifact_uris = train_data(df)
    if ti:
        ti.xcom_push(key="artifacts", value=artifact_uris)
    print("Artifact URIs:", artifact_uris)
    return artifact_uris

if __name__ == "__main__":
    start_training()
