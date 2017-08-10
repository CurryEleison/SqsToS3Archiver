from __future__ import print_function, unicode_literals
import argparse

from SqsToS3Archiver import QueueArchiver

def main():
    """
    The main method
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--queue-url", help="Queue URL of the queue to process", required=True)
    parser.add_argument("--region", help="AWS region", default="eu-west-1")
    parser.add_argument("--bucket-name", help="Bucket where files should go to", required=True)

    args = parser.parse_args()
    runner = QueueArchiver(args.queue_url, args.bucket_name, args.region)
    runner.run()


if __name__ == '__main__':
    main()
