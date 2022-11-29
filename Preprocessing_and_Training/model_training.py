import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
import pickle
import logging as log

class training:
    """
    This class will train the model and will output the pickle file in the end, which will
    later be used for getting the predictions for the input dataset.

    """

    def __init__(self,file_location):
        self.file_location = file_location

    def start_training(self):
        try:
            # Loading the dataset in pandas
            df = pd.read_csv(self.file_location)
            X = df.drop(columns = 'phishing')
            y = df['phishing']
            # Splitting the dataset for training and testing.
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=42)

            # creating a RF classifier
            clf = RandomForestClassifier(n_estimators = 100) 
            
            # Training the model on the training dataset
            # fit function is used to train the model using the training sets as parameters
            clf.fit(X_train, y_train)
            
            # Making a pickle file out of it.
            trainer = pickle.dump(clf, open("phishing.pkl", "wb")) 

            return trainer

        except Exception as e:
            log.warning('An error has occurred: {}'.format(e))

var = training('/config/workspace/Phishing_Classifier/Accepted_Dataset/phishing.csv')
var.start_training()