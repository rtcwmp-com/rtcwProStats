import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import accuracy_score

import joblib


import matplotlib.pyplot as plt

# Load the dataset
df = pd.read_csv("rt_training.csv")

# Encode the target variable 'result'
label_encoder = LabelEncoder()
df['result_encoded'] = label_encoder.fit_transform(df['result'])

# Define feature columns and target
feature_columns = [
    'mean_a', 'median_a', 'max_a', 'min_a', 'panz_a',
    'mean_b', 'median_b', 'max_b', 'min_b', 'panz_b'
]
X = df[feature_columns]
y = df['result_encoded']

# Split into training and test sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train a RandomForestClassifier
model = RandomForestClassifier(random_state=42)
model.fit(X_train, y_train)

# Evaluate the model
y_pred = model.predict(X_test)
accuracy = accuracy_score(y_test, y_pred)
print(f"Model Accuracy: {accuracy:.2f}")


# Export the model and label encoder using joblib
joblib.dump(model, "match_outcome_model.pkl")
joblib.dump(label_encoder, "label_encoder.pkl")

print("Model and label encoder have been successfully exported.")


# Reusable prediction function
def predict_match_outcome(mean_a, median_a, max_a, min_a, panz_a,
                          mean_b, median_b, max_b, min_b, panz_b):
    input_data = pd.DataFrame([[
        mean_a, median_a, max_a, min_a, panz_a,
        mean_b, median_b, max_b, min_b, panz_b
    ]], columns=feature_columns)
    prediction = model.predict(input_data)
    return label_encoder.inverse_transform(prediction)[0]

def visualize_features(model):
    importances = model.feature_importances_

    # Create a bar chart
    plt.figure(figsize=(10, 6))
    plt.barh(feature_columns, importances, color='skyblue')
    plt.xlabel("Feature Importance")
    plt.title("Feature Importance in Match Outcome Prediction")
    plt.gca().invert_yaxis()
    plt.tight_layout()
    plt.show()


# Example usage of the prediction function
visualize_features(model)

example_prediction = predict_match_outcome(1808.8, 1802.0, 1858, 1746, 1889,
                                           1787.2, 1841.0, 1910, 1603, 1843)
print(f"Predicted outcome for example match: {example_prediction}")

