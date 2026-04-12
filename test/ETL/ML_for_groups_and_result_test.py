import joblib
import warnings

# Suppress specific sklearn warning
warnings.filterwarnings("ignore", message="X does not have valid feature names")


# Load the model and label encoder
model = joblib.load("match_outcome_model.pkl")
label_encoder = joblib.load("label_encoder.pkl")

# Define input features as a list
input_features = [
    1808.8, 1802.0, 1858, 1746, 1889,  # Team A
    1687.2, 1841.0, 1910, 1603, 1843   # Team B
]

# Reshape input for prediction (1 sample, 10 features)
input_array = [input_features]

# Predict
prediction = model.predict(input_array)
result = label_encoder.inverse_transform(prediction)[0]

print(f"Predicted outcome: {result}")




# Predict probabilities
probabilities = model.predict_proba(input_array)[0]
class_probabilities = dict(zip(label_encoder.classes_, probabilities))

# Print results
for label, prob in class_probabilities.items():
    print(f"{label}: {prob:.2f}")
