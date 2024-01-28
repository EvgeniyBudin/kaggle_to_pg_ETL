FROM apache/airflow:2.7.1

COPY requirements.txt .
COPY .kaggle $HOME/home/airflow/.kaggle
RUN pip install -r requirements.txt