from flask import Flask,render_template,request 
import pickle
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
import logging

pickled_model = pickle.load(open('phishing.pkl', 'rb'))

app = Flask(__name__) ## defining flask name

@app.route('/') ## home route
def home():
    logging.basicConfig(filename='The_Logs\\log_files.log',
                        filemode='w',
                        level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(module)s---- %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger('')
    f = open('The_Logs\\log_files.log', 'w+')
    f.truncate()
    return render_template('home.html') ## At home route returning index.html to show

@app.route('/predict',methods=['POST']) ## on post request /predict 
def predict():
    if request.method=='POST': 
        data = request.form['message']  ## Requesting the path entered in the text field.
        preds = pd.read_csv(data) ## Loading it in the pandas
        if preds.shape[1] == 111 and preds.shape[0] == 88647:
            output = pickled_model.predict(preds)
            frame = pd.DataFrame(output)
            frame.to_csv('D:\\Final_Predictions.csv')
            return render_template('result.html',prediction=output)
        else:
            return render_template('result1.html')

if __name__ == "__main__":
    app.run(debug=True)
