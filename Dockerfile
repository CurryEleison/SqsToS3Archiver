FROM  python:3.6-alpine3.7

RUN apk --update add openssh && \
    rm -rf /var/lib/apt/lists/* && \
    rm /var/cache/apk/*  &&  \
    mkdir /sqs

WORKDIR /sqs

ADD requirements.txt .
ADD sqs2s3.py .
ADD SqsToS3Archiver.py .

RUN ls && \
    pip install -r requirements.txt

ENTRYPOINT [ "python3", "sqs2s3.py" ]
CMD ["--help"]