FROM datacovesprivate/airflow-airflow:latest

# pip install pandas==xx on /opt/datacoves/virtualenvs/main virtualenv
COPY requirements.txt .

RUN /opt/datacoves/virtualenvs/main/bin/pip install -r requirements.txt
