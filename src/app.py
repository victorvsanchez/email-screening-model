import streamlit as st
import pandas as pd
import threading
from simple_salesforce import Salesforce, SalesforceLogin
from transformers import pipeline
import pyarrow as pa
import pyarrow.parquet as pq
import datetime
import json
import plotly.express as px
import plotly.graph_objects as go
# login imports
import yaml
import streamlit_authenticator as stauth
from yaml.loader import SafeLoader


def get_emails():
    credentials = json.load(open("./credentials.json"))
    instance = credentials["instance"]
    username = credentials["username"]
    password = credentials["password"]
    token = credentials["security_token"]
    session_id, instance = SalesforceLogin(username=username, password=password, security_token=token)
    sf = Salesforce(instance=instance, session_id=session_id)

    timeframe = (datetime.datetime.now() - datetime.timedelta(minutes=3)).strftime("%Y-%m-%dT%H:%M:%S")

    query = sf.query("""
                     SELECT Id, FromAddress, TextBody, MessageDate 
                     FROM EmailMessage
                     WHERE MessageDate >= {}.000+0000 """.format(timeframe))

    return query

def get_predictions(query, parquet_file):
    df = pd.read_parquet(parquet_file)
    classifier = pipeline("text-classification", model="./models/model_without_weights/model_grouped", max_length=512, truncation=True)

    if query['totalSize'] > 0:
        for record in query['records']:
            try:
                result = classifier(record['TextBody'])
            except:
                result = [{'label': 'Outros', 'score': 0.1067645239830017}] #TODO: properly fix this later
                
            new_data = {'ID': record['Id'], 
                        'Data/Hora': record['MessageDate'], 
                        'Remetente': record['FromAddress'], 
                        'Descrição': record['TextBody'],
                        'Rótulo predito': result[0]['label'],
                        'Confiança': result[0]['score']}
            df.loc[len(df)] = new_data
        
    return df

def update_database():
    parquet_file = "./data/other_new_emails.parquet"
    query = get_emails()
    df = get_predictions(query, parquet_file)
    df.drop_duplicates(subset=['ID'], inplace=True)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, parquet_file)

def get_data():

    thread = threading.Thread(target=update_database,
                              name="update database - start")
    thread.start()

    data = pd.read_parquet('./data/other_new_emails.parquet')

    columns = ['ID',
               'Data/Hora',
               'Remetente',
               'Descrição',
               'Rótulo predito',
               'Confiança']

    data = data[columns]

    data.query("~`Descrição`.isnull() & ~(`Remetente`.str.contains('@copel.com') | `Remetente`.str.contains('@ccee.org.br'))", inplace=True)

    return data

def main():
    st.set_page_config(page_title="Emails de Triagem Copel", layout="wide")

    with open('./.secrets/users.yaml') as file:
        config = yaml.load(file, Loader=SafeLoader)

    authenticator = stauth.Authenticate(
        config['credentials'],
        config['cookie']['name'],
        config['cookie']['key'],
        config['cookie']['expiry_days'],
        config['preauthorized']
    )

    authenticator.login('Login', 'main')

    if st.session_state["authentication_status"]:
        with st.sidebar:
            authenticator.logout('Logout', 'main', key='unique_key')
            st.write(f'Bem vindo, *{st.session_state["name"]}*!')
        data = get_data()
        st.title("Classificação - Emails de Triagem")
        st.dataframe(data, use_container_width=True, hide_index=True)
        if st.button("Atualizar resultados"):
            st.rerun()
        ### Graphs
        # Group by type
        df_counts = data.groupby('Rótulo predito').count().reset_index()[['Rótulo predito', 'ID']]
        df_counts.rename(columns={'ID': 'Número'}, inplace=True)
        fig1 = px.bar(
                    x=df_counts["Número"],
                    y=df_counts["Rótulo predito"],
                    orientation='h')
        st.header("Número de rótulos preditos por categoria")
        st.plotly_chart(fig1, theme=None, use_container_width=False)
        # Group by date
        data['Data/Hora'] = pd.to_datetime(data['Data/Hora'])
        interval_counts = data.groupby(data['Data/Hora'].dt.floor('5Min')).size().reset_index(name='count')
        fig2 = px.line(interval_counts, x="Data/Hora", y="count")
        fig2.update_yaxes(rangemode="normal")
        st.header("Número de predições por 5 minutos")
        st.plotly_chart(fig2, theme=None, use_container_width=True)

    elif st.session_state["authentication_status"] is False:
        st.error('Username/password is incorrect')
    elif st.session_state["authentication_status"] is None:
        st.warning('Please enter your username and password')

if __name__ == '__main__':

    main()