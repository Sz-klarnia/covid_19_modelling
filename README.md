# Modelling COVID-19 pandemic in Poland

## Data Sources:
Our World In Data:
https://github.com/owid/covid-19-data/tree/master/public/data
Oxford Covid-19 Government Response Tracker:
https://github.com/OxCGRT/covid-policy-tracker
Google COVID-19 Mobility Reports:
https://www.google.com/covid19/mobility/

Training data contains informations from 12 countries: UK, Germany, Poland, Italy, Sweden, Netherlands, France, Spain, Portugal, Israel, Austria and Belgium. All of the chosen countries have fairly simmilar socioeconomical structures, and the differences were accounted for in data where needed. This approach enabled me to build dataset that is balanced and contains different containment strategies. 

## About

Project consists of three main Jupyter Notebooks, each showing a process of training a model to predict COVID-19 epidemics in Poland in three areas: new cases, hospitalized  patients, new deaths. Each model is based on an LSTM network with differing architectures. Evaluations are performed on a task of predicting course of the epidemics in next 60 days. All models achieved R2 score of 80 percent or more. Predictions follow original data values consistently. 


## Notes:
Sadly I didn't save all trials that led to this versions of both LSTM architectures and data preparation. This code shows only the final version of many trials. In the future I want to save my trials and errors to showcase the road that leads to final model
