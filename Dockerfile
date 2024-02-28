FROM python:3.10-slim

COPY ./src ./src
COPY ./.secrets ./.secrets
COPY ./pdm.lock .
COPY ./pyproject.toml .
COPY ./credentials.json .
COPY ./models ./models
COPY ./data/other_new_emails.parquet ./data/other_new_emails.parquet

RUN pip install --upgrade pip
RUN pip install pdm
RUN pdm install 

EXPOSE 8501

CMD pdm run streamlit run --server.port 8501 --server.enableCORS false ./src/app.py 