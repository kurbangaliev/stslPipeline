FROM apache/airflow

COPY ./docker/airflow/requirements.txt ./
RUN pip install pip==25.1.1
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install typing-extensions --upgrade
COPY ./dags ./dags

ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/dags"

