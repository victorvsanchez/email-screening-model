import json
from simple_salesforce import Salesforce, SalesforceLogin
import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from transformers import pipeline

parquet_file = "../data/new_emails.parquet"
credentials = json.load(open("../credentials.json"))
instance = credentials["instance"]
username = credentials["username"]
password = credentials["password"]
token = credentials["security_token"]

#initialize credential data
session_id, instance = SalesforceLogin(username=username, password=password, security_token=token)
sf = Salesforce(instance=instance, session_id=session_id)
instance_name = sf.sf_instance

#get emails sent in the last 5 minutes
timeframe = (datetime.datetime.now() - datetime.timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%S")

query = sf.query("""
                 SELECT Id, FromAddress, TextBody, MessageDate 
                 FROM EmailMessage
                 WHERE MessageDate >= {}.000+0000 """.format(timeframe))

df = pd.read_parquet(parquet_file)

classifier = pipeline("text-classification", model="../models/model_without_weights/model_grouped")

if query['totalSize'] > 0:
    for record in query['records']:
        try:
            result = classifier(record['TextBody'])
        except:
            result = [{'label': 'Outros', 'score': 0.1067645239830017}] #TODO: properly fix this later
            
        new_data = {'ID': record['Id'], 
                    'Data': record['MessageDate'], 
                    'Remetente': record['FromAddress'], 
                    'Descrição': record['TextBody'],
                    'Rótulo predito': result[0]['label'],
                    'Confiança': result[0]['score']}
        df.loc[len(df)] = new_data

    #save in parquet
    table = pa.Table.from_pandas(df)
    pq.write_table(table, parquet_file)