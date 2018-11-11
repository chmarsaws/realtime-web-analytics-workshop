/*********************************************************************************************************************
 *  Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                           *
 *                                                                                                                    *
 *  Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance        *
 *  with the License. A copy of the License is located at                                                             *
 *                                                                                                                    *
 *      http://aws.amazon.com/asl/                                                                                    *
 *                                                                                                                    *
 *  or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES *
 *  OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions    *
 *  and limitations under the License.                                                                                *
 *********************************************************************************************************************/
'use strict';
const AWS = require('aws-sdk');

var docClient = new AWS.DynamoDB.DocumentClient();

var METRIC_DETAILS_TABLE = process.env.METRIC_DETAILS_TABLE;
var METRIC_TABLE = process.env.METRIC_TABLE;
var DEBUG_EVENT = process.env.DEBUG_EVENT;
var DEBUG_RECORDS = process.env.DEBUG_RECORDS;
var PULL_FROM_STREAMS = process.env.PULL_FROM_STREAMS;

AWS.config.update({
    region: process.env.AWS_REGION,
    endpoint : 'https://dynamodb.' + process.env.AWS_REGION + '.amazonaws.com'
});

// 7 days.
var EXPIRE_TIME = 604800;

exports.handler = (event, context, callback) => {
    var uniqueMetricDetailKeys = new Map();
    var metricRecordsBatch;
    
    if(PULL_FROM_STREAMS=='Y') {
        //records pulled from Kinesis Streams from Lambda have this format
        metricRecordsBatch = event.Records.map((record) => JSON.parse(Buffer.from(record.kinesis.data, 'base64')));
    } else {
        //records pushed to Lambda from Kinesis Analytics has this format
        metricRecordsBatch = event.records.map((record) => JSON.parse(Buffer.from(record.data, 'base64')));        
    }
    if(DEBUG_EVENT == 'Y'){
        console.log(JSON.stringify(event,null,2));
    }
    if(DEBUG_RECORDS == 'Y'){
        console.log(JSON.stringify(metricRecordsBatch,null,2));
    }

    var total_items = 0;
    // Loop through all the records and retain a map of all the unique keys.
    for (let i = 0; i < metricRecordsBatch.length; i++ ) {
        if (validationCheck(metricRecordsBatch[i])) {
            var objKey = metricRecordsBatch[i].METRICTYPE + '|' + metricRecordsBatch[i].EVENTTIMESTAMP;
            if (uniqueMetricDetailKeys.has(objKey)) {
                // Already captured.
            } else {
                uniqueMetricDetailKeys.set(objKey, {
                    EVENTTIMESTAMP : metricRecordsBatch[i].EVENTTIMESTAMP,
                    METRICTYPE : metricRecordsBatch[i].METRICTYPE
                });
                total_items+=1;
            }
        }
    }
    console.log('Item count = ' + total_items);
    docClient.scan({ TableName : METRIC_TABLE }, (err, metricMetadata) => {
        if (!err) {
            // Create an array of all the detail records that match each key.
            uniqueMetricDetailKeys.forEach((value, key) => {
                var metricTypeSet = metricRecordsBatch.filter((item) => item.EVENTTIMESTAMP == value.EVENTTIMESTAMP && item.METRICTYPE == value.METRICTYPE);
                upsert(metricTypeSet, metricMetadata.Items);
            });
        } else {
            console.error('Unable to retrieve metric metadata from ' + METRIC_TABLE + ': ' + err);
        }
        // Determine the latest timestamp for each metric type.
        var LatestTimestampPerMetric = new Map();
        uniqueMetricDetailKeys.forEach((value, key) => {
            if (LatestTimestampPerMetric.has(value.METRICTYPE)){
                if (value.EVENTTIMESTAMP > LatestTimestampPerMetric[value.METRICTYPE]) {
                    LatestTimestampPerMetric[value.METRICTYPE] = value.EVENTTIMESTAMP;
                }
            } else {
                var idx = getMetricIndex(metricMetadata.Items, value.METRICTYPE);
                if (typeof metricMetadata.Items[idx] === 'undefined') {
                    console.log('metricMetadata.Items[idx] === \'undefined\'');
                    console.log(`idx: ${idx}`);
                    console.log(metricMetadata.Items[idx]);
                } else {
                    if (value.EVENTTIMESTAMP > metricMetadata.Items[idx].LatestEventTimestamp) {
                        LatestTimestampPerMetric.set(value.METRICTYPE, value.EVENTTIMESTAMP );
                    }
                }
            }
        });

        // Update the latest timestamp from each of the keys to the metrics table.
        LatestTimestampPerMetric.forEach((value,key) => {
            var MetricTableParams = {
                TableName: METRIC_TABLE,
                Key: { MetricType : key },
                UpdateExpression : 'set #a = :x',
                ExpressionAttributeNames : { '#a' : 'LatestEventTimestamp' },
                ExpressionAttributeValues : { ':x' : value }
            };
            docClient.update(MetricTableParams, (err,data) => {
                if (err) { console.error(err); }
            });
        });
    });
    callback(null, { records : event.records.map((record) => { return { recordId : record.recordId, result : 'Ok' }; })});
};

function upsert(metricTypeSet, allMetrics) {
    var firstItem = metricTypeSet[0];
    var ExpireTime = firstItem.EVENTTIMESTAMP + EXPIRE_TIME;
    var metricDetailParams = {
        TableName : METRIC_DETAILS_TABLE,
        Item : {
            MetricType : firstItem.METRICTYPE,
            EventTimestamp : firstItem.EVENTTIMESTAMP,
            ExpireTime : ExpireTime,
            MetricDetails : metricTypeSet
        },
        ConditionExpression : 'attribute_not_exists(MetricType)'
    };

    try {
        docClient.put(metricDetailParams, function (err, data) {
            if (err) {
                if (err.code == "ConditionalCheckFailedException") {
                    amendMetric(metricTypeSet,allMetrics);
                } else {
                    console.error('Error updating metric detail table: ' + JSON.stringify(err,null,2));
                }
            }
        });
    } catch (err) {
        console.error('Unable to save records to DynamoDB: ', err);
    }
};

function amendMetric(metric_list,allMetrics) {
    var params = {
      TableName: METRIC_DETAILS_TABLE,
      KeyConditionExpression: 'MetricType = :hkey and EventTimestamp = :rkey',
      ExpressionAttributeValues: {
        ':hkey': metric_list[0].METRICTYPE,
        ':rkey': metric_list[0].EVENTTIMESTAMP
      }
    };

    // Get the existing data from METRIC_DETAILS_TABLE.
    docClient.query(params, (err, itemToAmend) => {
        if (!err) {
            var detailsToAmend = itemToAmend.Items[0].MetricDetails;
            var metricIndex = getMetricIndex(allMetrics,metric_list[0].METRICTYPE);
            // If metric is not found, don't do anything.
            if (metricIndex === -1) {
                return
            }
            var amendmentStrategy = allMetrics[metricIndex].AmendmentStrategy;
            var isWholeNumberMetric = allMetrics[metricIndex].IsWholeNumber;
            var isSet = allMetrics[metricIndex].IsSet;
            //console.log('metric:', allMetrics[metricIndex]);
            //console.log('amendmentStrategy: %s', amendmentStrategy);
            if(isSet == true){
                switch (amendmentStrategy) {
                    case 'add':
                        // For each item, find a match and add the values or add a new item.
                        metric_list.map( (item) => {
                            var detailIndex = getMetricDetailIndex(detailsToAmend, item.METRICITEM);
                            // Same metric exists in existing set.
                            if (detailIndex > -1) {
                                if (isWholeNumberMetric){
                                    detailsToAmend[detailIndex].UNITVALUEINT = detailsToAmend[detailIndex].UNITVALUEINT + item.UNITVALUEINT;
                                } else {
                                    detailsToAmend[detailIndex].UNITVALUEFLOAT = detailsToAmend[detailIndex].UNITVALUEFLOAT + item.UNITVALUEFLOAT;
                                }
                            } else {
                                detailsToAmend.push(item);
                            }
                        });
                    // If it exists, replace with updated value, if it is new, append it.
                    case 'replace_existing':
                        // For each item, find a match.
                        metric_list.map( (item) => {
                            var detailIndex = getMetricDetailIndex(detailsToAmend, item.METRICITEM);
                            // Same metric exists in existing set.
                            if (detailIndex > -1) {
                                detailsToAmend[detailIndex] = item;
                            } else {
                                detailsToAmend.push(item);
                            }
                        });
                        break;
                    case 'replace':
                        detailsToAmend = metric_list;
                        break;
                    default:
                        console.error('Unexpected amemdment strategy \'' + amendmentStrategy + '\'');
                }
            } else {
                switch (amendmentStrategy) {
                    case 'add':
                        if (isWholeNumberMetric){
                            detailsToAmend[0].UNITVALUEINT = detailsToAmend[0].UNITVALUEINT + metric_list[0].UNITVALUEINT;
                        } else {
                            detailsToAmend[0].UNITVALUEFLOAT = detailsToAmend[0].UNITVALUEFLOAT + metric_list[0].UNITVALUEFLOAT;
                        }
                        break;
                    case 'replace':
                    case 'replace_existing':
                        detailsToAmend = metric_list;
                        break;
                    default:
                        console.error('Unexpected amemdment strategy \'' + amendmentStrategy + '\'');                        
                }
            }
            if (detailsToAmend) {
                var ExpireTime = metric_list[0].EVENTTIMESTAMP + EXPIRE_TIME;
                var amendedParams = {
                    TableName : METRIC_DETAILS_TABLE,
                    Item : {
                        MetricType : metric_list[0].METRICTYPE,
                        EventTimestamp : metric_list[0].EVENTTIMESTAMP,
                        ExpireTime : ExpireTime,
                        MetricDetails : detailsToAmend
                    }
                };
                docClient.put(amendedParams, (err,data) => {
                    if (err) {
                        console.error('Error amending record:' + err + ' data ='  + JSON.stringify(data,null,2));
                    }
                });
            }
        } else {
            // Could not get details.
            console.error('Could not get expected results from the details table.', err);
        }
    });
};

function getMetricDetailIndex(searchArray, metricItem) {
  for (let i = 0; i < searchArray.length; i++) {
    if (searchArray[i].METRICITEM == metricItem) {
      return i;
    }
  }
  // Not found
  return -1;
};

function getMetricIndex(searchArray, metricType) {
    for (let i = 0; i < searchArray.length; i++) {
        if (searchArray[i].MetricType == metricType) {
            return i;
        }
    }
    // Not found.
    return -1;
};

function validationCheck(metricRecord) {
    try {
        return metricRecord.METRICTYPE != null && metricRecord.EVENTTIMESTAMP > 0;
    } catch (err) {
        console.error('Invalid metric record ' + JSON.stringify(metricRecord,null,2));
        return false;
    }
};
