FROM        python:3.6-alpine

EXPOSE      10001
WORKDIR     /app
ENV         PYTHONPATH=.
ENV         PYTHONUNBUFFERED=true

COPY        ./Pipfile* /app/
RUN         pip install -U pipenv && pipenv install --system --deploy

COPY        . /app/

ENTRYPOINT ["python3", "s3_proxy/main.py"]
