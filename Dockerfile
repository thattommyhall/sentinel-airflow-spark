FROM datamechanics/spark:3.1.1-hadoop-3.2.0-java-11-scala-2.12-python-3.8-latest

WORKDIR /opt/application/

ENV PYSPARK_MAJOR_PYTHON_VERSION=3

ENV PYTHONFAULTHANDLER=1 \
  PYTHONUNBUFFERED=1 \
  PYTHONHASHSEED=random \
  PIP_NO_CACHE_DIR=off \
  PIP_DISABLE_PIP_VERSION_CHECK=on \
  PIP_DEFAULT_TIMEOUT=100 
  
RUN pip install 'poetry==1.1.7'

COPY poetry.lock pyproject.toml ./
RUN poetry config virtualenvs.create false \
  && poetry install --no-dev --no-interaction --no-ansi

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY schema.yaml .
COPY *.py ./

