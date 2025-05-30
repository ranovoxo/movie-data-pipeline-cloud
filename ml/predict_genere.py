import joblib
import numpy as np
from preprocess_text import clean_text

# Load saved artifacts
clf = joblib.load('ml/genre_model.joblib')
vectorizer = joblib.load('ml/vectorizer.joblib')
mlb = joblib.load('ml/label_binarizer.joblib')

def predict_genres(overview_text):
    cleaned = clean_text(overview_text)
    X = vectorizer.transform([cleaned])
    y_pred = clf.predict(X)
    genres = mlb.inverse_transform(y_pred)
    # inverse_transform returns a tuple of tuples, so unwrap
    return genres[0] if genres else ()

# Example usage
if __name__ == "__main__":
    example_overview = "A spaceship crew explores a distant galaxy filled with aliens."
    predicted_genres = predict_genres(example_overview)
    print("Predicted genres:", predicted_genres)
