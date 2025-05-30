import joblib
import pandas as pd
from sklearn.preprocessing import MultiLabelBinarizer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.multiclass import OneVsRestClassifier
from preprocess_text import clean_text

# Load your dataset
df = pd.read_csv('path/to/movies.csv')  # Update path accordingly

# Preprocess genres column into list
df['genre_list'] = df['genres'].fillna("").apply(lambda x: x.split('|'))

# Clean the overview text
df['cleaned_overview'] = df['overview'].fillna("").apply(clean_text)

# Binarize labels
mlb = MultiLabelBinarizer()
y = mlb.fit_transform(df['genre_list'])

# Vectorize text
vectorizer = TfidfVectorizer(max_features=5000)
X = vectorizer.fit_transform(df['cleaned_overview'])

# Train-test split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train model
clf = OneVsRestClassifier(LogisticRegression(max_iter=1000))
clf.fit(X_train, y_train)

# Save artifacts for inference
joblib.dump(clf, 'ml/genre_model.joblib')
joblib.dump(vectorizer, 'ml/vectorizer.joblib')
joblib.dump(mlb, 'ml/label_binarizer.joblib')

print("Training complete and models saved.")
