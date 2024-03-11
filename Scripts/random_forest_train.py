from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
import pandas as pd

def train(x, y):
    # Split the data into training and testing sets
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=42)
    # Initialize the Random Forest classifier
    rf_classifier = RandomForestClassifier(n_estimators=100, random_state=42)
    # Train the classifier on the training data
    rf_classifier.fit(x_train, y_train)

    return rf_classifier, x_test, y_test


def main():
    file_path = 'cleaned_data.csv'
    # Assuming the file has a header and uses ',' as delimiter
    df = pd.read_csv(file_path, delimiter=',')
    y = df['home_outcome']
    x = df.drop('home_outcome', axis=1)
    # Assuming X is your feature matrix and y is your target variable
    rf_classifier, x_test, y_test = train(x, y)
    # Make predictions on the test data
    y_pred = rf_classifier.predict(x_test)
    # Evaluate the model
    accuracy = accuracy_score(y_test, y_pred)
    print("Accuracy:", accuracy)


if __name__ == "__main__":
    main()
