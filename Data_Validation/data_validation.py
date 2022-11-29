import csv
import pandas as pd
import logging
import shutil

class validation:

    """
    This class will validate the the dataset for the training, it will check whether the dataset has the desired number of 
    columns and datatypes.

    Version 1.0
    Revision = None

    """
    try:
        def __init__(self, dataset_path):
            self.dataset_path = dataset_path

        def data_validation(self): # This is the method that will make sure whether to accept or reject the dataset.
            df = pd.read_csv(self.dataset_path)
            source = self.dataset_path
            destination1 = "/config/workspace/Phishing_Classifier/Accepted_Dataset/phishing.csv"
            destination2 = '/config/workspace/Phishing_Classifier/Rejected_Dataset/phishing.csv'
            rows = str(df.shape[0])
            cols = str(df.shape[1])
            if cols == '112' and rows == '88647':
                shutil.copy(source, destination1)

            else:
                shutil.copy(source, destination2)
                print('The file is not valid! Please check the number of columns, rows or both.')
    except Exception as e:
        self.logging('An error has occurred : {}'.format(e))

#run = validation('Phishing_Classifier/dataset_full.csv')
#run.data_validation()