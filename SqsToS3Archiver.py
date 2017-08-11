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

def issnswrapped(logitem):
    """
    Checks if the message is in an SNS wrapper
    """
    return 'Type' in logitem and logitem['Type'] == 'Notification' and 'Message' in logitem and 'MessageId' in logitem

def getdataitem(logitembody):
    """
    Figures out if the logitem is a raw item or if it has sns metadata
    Returns the data sans SNS wrapper
    """
    logitem = json.loads(logitembody)
    retval = logitem
    if issnswrapped(logitem):
        retval = json.loads(logitem['Message'])
    return retval


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


def getarchivestream(dataitem):
    """
    Takes a data item and decides which logstream it should go into
    """
    logstream = 'multiplex'
    if isnormaldataitem(dataitem):
        logstream = "{0}_{1}".format(dataitem['Table'], dataitem['Action'])
    return logstream



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
        # initialsize = self.estimate_queuesize()
        # (initalsize, timeout) = self.getqueueparams()
        queueparams = self.getqueueparams()
        initialsize = queueparams['ApproximateNumberOfMessages']
        timeout = queueparams['VisibilityTimeout']
        estimatedsize = initialsize
        iterations = 0
        maxtime = timeout / 2

        # Don't run longer than the max which is half the timeout
        # Next condition breaks off if we have gotten very many iterations
        # Next condition breaks if we have the item count we expected
        # and not a lot remains
        # The complexity is there to avoid being stuck processing
        # when the queue is running empty and to ensure we don't
        # keep queue items in the air longer than intended
        while (datetime.datetime.utcnow() - self.startuptime).total_seconds() < maxtime \
                and ((lastcount > 0 and (iterations < estimatedsize)) \
                or (lastcount > 1 and (totalcount <= initialsize))):
            lastcount = self.pullbatchfromqueue()
            totalcount = totalcount + lastcount
            estimatedsize = estimatedsize - lastcount
            iterations = iterations + 1
            # If we have a lot of records  we will occasionally check if we should flush
            # some streams
            if iterations > self.maxstreamsize and iterations % 10 == 0:
                for strmkey in self.streams.keys():
                    if len(self.streams[strmkey]) > self.maxstreamsize:
                        self.archivestream(strmkey, self.streams[strmkey])
                        self.streams[strmkey] = list()



        streamkeys = self.streams.keys()
        for strm in streamkeys:
            self.archivestream(strm, self.streams[strm])
            del self.streams[strm]

        
        report = dict()
        report['Iterations'] = iterations
        report['ArchivedCount'] = totalcount
        report['EstimatedInitalSize'] = initialsize
        return report


        


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


    def getqueueparams(self):
        """
        Find the initial size and visibility timeout of the queue
        """
        resp = self.sqsclient.get_queue_attributes(QueueUrl=self.queueurl, \
                AttributeNames=['ApproximateNumberOfMessages', 'VisibilityTimeout'])
        retval = dict()
        retval['ApproximateNumberOfMessages'] = long(resp['Attributes']['ApproximateNumberOfMessages'])
        retval['VisibilityTimeout'] = long(resp['Attributes']['VisibilityTimeout'])
        return retval


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
