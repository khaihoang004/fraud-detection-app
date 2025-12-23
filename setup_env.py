import os
import joblib
from sklearn.linear_model import LogisticRegression
import numpy as np

def main():
    # 1. Create models directory
    if not os.path.exists("models"):
        os.makedirs("models")
        print("Created 'models' directory.")

    # 2. Create and train a dummy model
    print("Training dummy model...")
    # Mock data: [Amount] -> [0/1]
    X = np.array([[10], [100], [1000], [10000], [50]]).reshape(-1, 1)
    y = np.array([0, 0, 0, 1, 0]) # High amount = fraud (dummy logic)
    
    model = LogisticRegression()
    model.fit(X, y)
    
    # 3. Save model
    model_path = "models/fraud_model.pkl"
    joblib.dump(model, model_path)
    print(f"Dummy model saved to {model_path}")

    print("\nSetup Complete!")
    print("You can now proceed with the instructions in RUNNING.md")

if __name__ == "__main__":
    main()
