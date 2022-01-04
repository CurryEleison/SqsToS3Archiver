FROM python:3.9-alpine

LABEL maintainer="petersen@temp.dk"

RUN apk --update add openssh && \
    rm -rf /var/lib/apt/lists/* && \
    rm /var/cache/apk/*  &&  \
    mkdir /app

COPY . /app
WORKDIR /app

RUN pip install -r requirements.txt

ENTRYPOINT [ "python3", "sqs2s3.py" ]
CMD ["--help"]
