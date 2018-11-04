#############################################################################
## cwmetricstream.py - A Lambda function that reads from a DynamoDB stream ##
## and pushes CloudWatch metrics to different event count namespaces.      ##
## ----------------------------------------------------------------------- ##
## Set the following environment variables:                                ##
## METRICS_TABLE = DynamoDB table containing all metric types              ##
############################################################################# 

import boto3
import sys
from os import environ
from datetime import datetime

tablename = environ['METRICS_TABLE']
region= environ['AWS_REGION']

DynamoDBClient = boto3.client('dynamodb', region_name=region)
allMetrics = DynamoDBClient.scan(
    TableName=tablename,
    Select='ALL_ATTRIBUTES'
)

def isWholeNumber(metric_type):
    for metric_meta in allMetrics['Items']:
        if metric_meta['MetricType']['S'] == metric_type:
            return metric_meta['IsWholeNumber']['BOOL'] == True

def prettyUp(s):
    pretty = ""
    parts = s.split("_")
    for part in parts:
        pretty = pretty + part[:1].upper() + part[1:]
    return pretty

def lambda_handler(event, context):
    for record in event['Records']:
        metricData = []
        try:
            event_timestamp = record['dynamodb']['NewImage']['EventTimestamp']['N']
            timestamp = float(event_timestamp) / 1000
            event_time = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%dT%H:%M:%S')
            print('event_time=' + event_time)
            metric_type =  record['dynamodb']['NewImage']['MetricType']['S']
            is_metric_int = isWholeNumber(metric_type)
            for metric_detail in record['dynamodb']['NewImage']['MetricDetails']['L']:
                if is_metric_int:
                    value = float(metric_detail['M']['UNITVALUEINT']['N'])
                else:
                    value = float(metric_detail['M']['UNITVALUEFLOAT']['N'])
                if metric_detail['M']['METRICITEM']['S'] == 'null':
                    metricname = metric_type
                else:
                    metricname = metric_detail['M']['METRICITEM']['S']
                metricDataItem={
                    'MetricName': metricname,
                    'Timestamp': event_time,
                    'Value': value,
                    'Unit': 'None',
                    'StorageResolution': 1
                }
                metricData.append(metricDataItem)
            namespace = prettyUp(metric_type)
            print('metrics to cwc = {}'.format(metricData))
            cwc=boto3.client('cloudwatch')
            response = cwc.put_metric_data(Namespace=namespace,MetricData=metricData)
            print(response)
        except: # skip when records are removed via TTL
            print('Skip removed records ({})'.format(sys.exc_info()[0]))
            #raise
    return 'Successfully processed records.'