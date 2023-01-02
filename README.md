# Phishing-Domain-Classifier

Phishing is a type of fraud in which an attacker impersonates a reputable company or
person in order to get sensitive information such as login credentials or account
information via email or other communication channels. Phishing is popular among
attackers because it is easier to persuade someone to click a malicious link that appears
to be authentic than it is to break through a computer's protection measures.
The mail goal is to predict whether the domains are real or malicious.

# Dataset
https://data.mendeley.com/datasets/72ptz43s9v/1

Full variant - dataset_full.csv
Short description of the full variant dataset:
Total number of instances: 88,647
Number of legitimate website instances (labeled as 0): 58,000
Number of phishing website instances (labeled as 1): 30,647
Total number of features: 111

# Data Validation

The model will run on a pre defined database only, a certain number of columns and rows must be there in order for the Machine Leaning algorithm to work.

# Data Preprocessing

If there are any null values, they will be dropped.

# Training

Random Forest Classifier was used out of several classifier algorithms, giving accuracy around 96.63%

# Deployment Link in AWS
http://phishy-env.eba-3cvpmppf.ap-northeast-1.elasticbeanstalk.com/
