# SqsToS3Archiver
Archives json messages from an SQS queue to S3

Very simple program to clean out an SQS queue of json messages and archive 
the messages to S3. If the messages are from SNS it will strip the SNS 
metainfo from the message. If messages conform to a particular format it will 
spread them out to separate files. 

Don't expect the organization to work for you, but if you think you might like 
the program to work differently I'd love to hear how you might want to use it.

It's written for python2.7, but might work in python3.

## Installation
```
git clone git@github.com:CurryEleison/SqsToS3Archiver.git
cd SqsToS3Archiver
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
python sqs2s3.py --region eu-west-1 --bucket-name mybucket --queue-url https://sqs.eu-west-1.amazonaws.com/123456789012/myqueue
```
