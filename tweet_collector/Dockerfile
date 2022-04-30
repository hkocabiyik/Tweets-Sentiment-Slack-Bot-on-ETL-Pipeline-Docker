FROM python:3.8-slim-buster

WORKDIR /app

COPY . /app

RUN pip install --trusted-host pypi.python.org -r requirements.txt

CMD ["python", "TweetReader.py"]
