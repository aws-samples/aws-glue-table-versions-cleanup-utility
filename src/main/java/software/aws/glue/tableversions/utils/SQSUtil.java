//Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
//SPDX-License-Identifier: MIT-0

package software.aws.glue.tableversions.utils;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;

public class SQSUtil {

	/**
	 * This method send a message to SQS queue.
	 * @param sqs
	 * @param queueURI
	 * @param message
	 * @param executionBatchId
	 * @param databaseName
	 * @return
	 */
	public boolean sendTableSchemaToSQSQueue(AmazonSQS sqs, String queueURI, String message, long executionBatchId,
			String databaseName) {
		int statusCode = 400;
		boolean messageSentToSQS = false;
		Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
		messageAttributes.put("ExecutionBatchId", new MessageAttributeValue().withDataType("String.ExecutionBatchId")
				.withStringValue(Long.toString(executionBatchId)));
		SendMessageRequest req = new SendMessageRequest().withQueueUrl(queueURI).withMessageBody(message)
				.withMessageGroupId(databaseName).withMessageAttributes(messageAttributes);
		try {
			SendMessageResult sendMsgRes = sqs.sendMessage(req);
			statusCode = sendMsgRes.getSdkHttpMetadata().getHttpStatusCode();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Exception thrown while writing message to SQS. " + e.getLocalizedMessage());
		}
		if (statusCode == 200) {
			messageSentToSQS = true;
		} else
			System.out.printf("Cannot write Table schema %s to SQS queue. \n", message);
		return messageSentToSQS;
	}

}
