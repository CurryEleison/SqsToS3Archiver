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
        """
        Initializer
        """
        self.receipt_handle = receipt_handle
        self.messageobject = messageobject

    def getjson(self):
        """
        Returns msg as a json string.
        """
        return json.dumps(flattendataitem(self.messageobject))


class QueueArchiver:
    """
    Takes a queue from SQS and archives it to S3
    Items that can't be processed as expected are ignored
    and will make their way to the dead letter queue
    eventually.
    Don't expect this to work well against seriously busy
    queues although some steps have been taken to handle
    reasonably busy or large queues
    By default will flush messages in batches of ~10000 records
    if there are a lot
    """

    def __init__(self, queueurl, bucketname, region, prefix='customerlogarchive'):
        """
        Initializer
        """
        self.maxstreamsize = 10000 #How may items we are OK keeping in memory at a time

        self.queueurl = queueurl
        self.bucketname = bucketname
        self.region = region
        self.prefix = prefix

        self.sqsclient = boto3.client('sqs', region_name=self.region)
        self.s3client = boto3.client('s3', region_name=self.region)
        self.streams = dict()
        self.startuptime = datetime.datetime.utcnow()
        random.seed()


    def run(self):
        """
        Main loop to pulle messages off the queue
        """
        lastcount = 9999 # just something big here to initialize the process
        totalcount = 0 # to keep track of how many msgs we process
        queueparams = self.getqueueparams()
        initialsize = queueparams['ApproximateNumberOfMessages'] #how many msgs we start with
        timeout = queueparams['VisibilityTimeout'] # how long msgs are keps in-flight
        estimatedsize = initialsize # running esitmate of how many msgs are left
        iterations = 0 # running count of how many times we have looped
        maxtime = timeout / 2 # the max amnt of time we want to keep processing

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
            # some streams. Check every 10 loops after we go above max msg count
            if totalcount > self.maxstreamsize and iterations % 10 == 0:
                for strmkey in self.streams.keys():
                    if len(self.streams[strmkey]) > self.maxstreamsize:
                        self.archivestream(strmkey, self.streams[strmkey])
                        self.streams[strmkey] = list()

        # OK. Done pulling msgs from queues. Let's archive what we still have
        streamkeys = list(self.streams.keys())
        for strm in streamkeys:
            self.archivestream(strm, self.streams[strm])
            del self.streams[strm]

        # Finally send back a report of what we did
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
            except: # Really ought to be more specific about the exceptions we actually expect
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
                gz.write(itm.getjson().encode('utf-8'))
                gz.write(b'\n')
            gz.close()
            self.s3client.put_object(Bucket=self.bucketname, Key=key, \
                ContentType='application/json', ContentEncoding='gzip',\
                Body=gz_body.getvalue())
            for itm in singlestream:
                self.sqsclient.delete_message(QueueUrl=self.queueurl, \
                        ReceiptHandle=itm.receipt_handle)


    def getqueueparams(self):
        """
        Find the initial size and visibility timeout of the queue
        """
        resp = self.sqsclient.get_queue_attributes(QueueUrl=self.queueurl, \
                AttributeNames=['ApproximateNumberOfMessages', 'VisibilityTimeout'])
        retval = dict()
        retval['ApproximateNumberOfMessages'] = int(resp['Attributes']['ApproximateNumberOfMessages'])
        retval['VisibilityTimeout'] = int(resp['Attributes']['VisibilityTimeout'])
        return retval


    def outputpath(self, streamkey):
        """
        Returns S3 prefix for log dir.
        Very important not to start with a /
        """
        return '{2}/{1}/{0:%Y}/{0:%m}/{0:%d}/'.format(self.startuptime, streamkey, self.prefix)

    def outputfilename(self, streamkey):
        """
        Returns the objejt key wo prefix of the s3 object
        """
        randint = random.randint(1, 4000000000)
        return '{0:%Y}-{0:%m}-{0:%d}-{0:%H}-{0:%M}-{1}-{2:08x}.json.gz'.format(self.startuptime, \
                streamkey, randint)
