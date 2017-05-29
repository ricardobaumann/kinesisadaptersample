// Copyright 2012-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// Licensed under the Apache License, Version 2.0.
package kinesisadaptersample;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import kinesisadaptersample.StreamsRecordProcessor;

public class StreamsRecordProcessorFactory implements IRecordProcessorFactory {

    private final String tableName;

    public StreamsRecordProcessorFactory(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public IRecordProcessor createProcessor() {
        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.standard().build();
        return new StreamsRecordProcessor(dynamoDBClient, tableName);
    }

}
