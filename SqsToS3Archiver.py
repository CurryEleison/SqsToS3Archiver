from __future__ import unicode_literals
import boto3
import json
import random
import datetime
from io import BytesIO
from gzip import GzipFile
import os.path

# first a couple of global methods

def isnormaldataitem(dataitem):
    """
    Detects if the data item is in standard form
    """
    return 'Table' in dataitem and 'Action' in dataitem and 'Data' in dataitem

def flattendataitem(dataitem):
    """
    This could easily be more elegant, but
    """
    # If it's a dataitem for the multiplex stream we don't flatten it
    if not isnormaldataitem(dataitem):
        return dataitem

    # Nice normalized dataitems get to be flattened of course
    flattened = dict()
    innerdata = dict()
    for k in dataitem.keys():
        if not isinstance(dataitem[k], dict):
            flattened[k] = dataitem[k]
        elif k == 'Data' and isinstance(dataitem[k], dict):
            innerdata = dataitem[k]
    for k in innerdata.keys():
        flattened[k] = innerdata[k]
    return flattened



class StreamItem:
    """
    Holds an item to be flushed to S3 while we are processing
    """
    def __init__(self, receipt_handle, messageobject):
        self.receipt_handle = receipt_handle
        self.messageobject = messageobject

    def getjson(self):
        return json.dumps(flattendataitem(self.messageobject))

class QueueArchiver:
    """
    Takes a queue from SQS and archives it to S3
    Items that can't be processed as expected are ignored
    and will make their way to the dead letter queue
    eventually.
    Don't expect this to work well against seriously busy
    queues although some steps have been taken to handle
    extreme cases
    TODO: read visibility timeout of queue and ensure we
    don't run for longer than that
    """

    def __init__(self, queueurl, bucketname, region):
        """
        Initializer
        """
        self.queueurl = queueurl
        self.bucketname = bucketname
        self.region = region
        self.sqsclient = boto3.client('sqs', region_name=self.region)
        self.s3client = boto3.client('s3', region_name=self.region)
        self.startuptime = datetime.datetime.utcnow()
        self.streams = dict()
        random.seed()
        self.maxstreamsize = 10000


    def run(self):
        """
        Main loop to pulle messages off the queue
        """
        lastcount = 9999 # just something big here to initialize the process
        totalcount = 0
        initialsize = self.estimate_queuesize()
        estimatedsize = initialsize
        iterations = 0

        # First condition breaks off if we have gotten very many iterations
        # Second condition breaks if we have the item count we expected
        # and not a lot remains
        # The complexity is there to avoid being stuck processing 
        # when the queue is running empty
        while (lastcount > 0 and (iterations < estimatedsize)) \
                or (lastcount > 1 and (totalcount <= initialsize)):
            lastcount = self.pullbatchfromqueue()
            totalcount = totalcount + lastcount
            estimatedsize = estimatedsize - lastcount
            iterations = iterations + 1
            # Add something to flush streams that are getting too big

        for strm in self.streams.keys():
            self.archivestream(strm, self.streams[strm])


        


    def pullbatchfromqueue(self):
        """
        Pull items from queue for analysis
        """
        retval = 0
        response = self.sqsclient.receive_message(QueueUrl=self.queueurl, \
                MaxNumberOfMessages=10)
        if 'Messages' in response:
            self.addsqsresponsestostream(response)
            retval = len(response['Messages'])
        return retval

    def addsqsresponsestostream(self, sqsresponse):
        """
        Takes a response batch and sorts it into streams
        """
        for msg in sqsresponse['Messages']:
            # If the item fails to parse out as expected we just pass over it
            # Failed items will eventually make their way to the dead letter queue
            try:
                itm = StreamItem(msg['ReceiptHandle'], getdataitem(msg['Body']))
                streamname = getarchivestream(itm.messageobject)
                if not streamname in self.streams:
                    self.streams[streamname] = list()
                self.streams[streamname].append(itm)
            except: # Really ought to be more sepcific about the exceptions we actually expect
                pass

    def archivestream(self, streamname, singlestream):
        """
        Takes a single stream and flushes it
        """
        if len(singlestream) > 0:
            gz_body = BytesIO()
            gz = GzipFile(None, 'wb', 9, gz_body)
            fpath = self.outputpath(streamname)
            fname = self.outputfilename(streamname)
            key = os.path.join(fpath, fname)
            for itm in singlestream:
                gz.write(itm.getjson())
                gz.write('\n')
            gz.close()
            self.s3client.put_object(Bucket=self.bucketname, Key=key, \
                ContentType='application/json', ContentEncoding='gzip',\
                Body=gz_body.getvalue())
            for itm in singlestream:
                self.sqsclient.delete_message(QueueUrl = self.queueurl, \
                        ReceiptHandle = itm.receipt_handle)




    def estimate_queuesize(self):
        """
        Get a rough idea of how many items are visible in the queue atm
        """
        resp = self.sqsclient.get_queue_attributes(QueueUrl=self.queueurl, \
                AttributeNames=['ApproximateNumberOfMessages'])
        return long(resp['Attributes']['ApproximateNumberOfMessages'])

    def outputpath(self, streamkey):
        """
        Returns S3 prefix for log dir.
        Very important not to start with a /
        """
        return 'customerlogarchive/{1}/{0:%Y}/{0:%m}/{0:%d}/'.format(self.startuptime, streamkey)

    def outputfilename(self, streamkey):
        """
        Returns the objejt key wo prefix of the s3 object
        """
        randint = random.randint(1, 4000000000)
        return '{0:%Y}-{0:%m}-{0:%d}-{0:%H}-{0:%M}-{1}-{2:08x}.json.gz'.format(self.startuptime, \
                streamkey, randint)
