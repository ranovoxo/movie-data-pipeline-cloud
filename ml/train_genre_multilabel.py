import joblib
import pandas as pd
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
    joblib.dump(clf, 'ml/genre_model.joblib')
    print("Training complete and model genre_model.joblib saved.")
    joblib.dump(vectorizer, 'ml/vectorizer.joblib')
    print("Training complete and model vectorizer.joblib saved.")
    joblib.dump(mlb, 'ml/label_binarizer.joblib')
    print("Training complete and model label_binarizer.joblib saved.")

def start_training():
    df = get_data()
    train_data(df)

if __name__ == "__main__":
    start_training()