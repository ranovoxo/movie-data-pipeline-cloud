import joblib

def save_artifact(obj, filename):
    joblib.dump(obj, filename)

def load_artifact(filename):
    return joblib.load(filename)
