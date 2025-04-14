package pl.jacekgajek.sqs.consumer

import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.model.DeleteMessageRequest
import aws.sdk.kotlin.services.sqs.model.DeleteMessageResponse
import aws.sdk.kotlin.services.sqs.model.ListQueuesResponse
import aws.sdk.kotlin.services.sqs.model.Message
import aws.sdk.kotlin.services.sqs.model.ReceiveMessageRequest
import aws.sdk.kotlin.services.sqs.model.ReceiveMessageResponse
import io.kotest.core.spec.style.BehaviorSpec
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.mockk.coEvery
import io.mockk.coVerifyAll
import io.mockk.coVerifyOrder
import io.mockk.mockk
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.withTimeoutOrNull
import pl.jacekgajek.sqs.consumer.user.DomainEvent
import java.util.UUID
import kotlin.time.Duration.Companion.milliseconds

private const val QUEUE_NAME = "queue-name"
private const val QUEUE_URL = "queue/url"
private const val FLOW_TIME_OUT = 500L
private const val POLL_RATE = 5L
private const val TOPIC = "tenants"

class SqsSnsConsumerTest : BehaviorSpec({
	given("External services are mocked") {
		`when`("SQS returns two items") {
			then("Then those items are emitted.") {
				// Given
				val sqsClient = mockk<SqsClient>()
				val snsSubscriber = mockk<SnsSubscriber>()

				val response1 = ReceiveMessageResponse {
					messages = listOf(Message { receiptHandle = "r1"; body = makeSnsMessageBody("name1") })
				}
				val response2 = ReceiveMessageResponse {
					messages = listOf(Message { receiptHandle = "r2"; body = makeSnsMessageBody("name2") })
				}

				coEvery { sqsClient.receiveMessage(any()) } returns response1 andThen response2 andThen ReceiveMessageResponse {}
				coEvery { sqsClient.deleteMessage(any()) } returns DeleteMessageResponse {}
				coEvery { snsSubscriber.subscribe(any(), any()) } returns Result.success(QUEUE_URL)
				coEvery { sqsClient.listQueues(any()) } returns ListQueuesResponse { queueUrls = listOf(QUEUE_URL) }

				val sqsConsumer = SqsSnsConsumer(sqsClient, snsSubscriber)

				// When
				val items = runTest(sqsConsumer)

				// Then
				items shouldHaveSize 2
				items[0].data shouldBe """{"name": "name1"}"""
				items[1].data shouldBe """{"name": "name2"}"""
				coVerifyOrder {
					sqsClient.receiveMessage(ReceiveMessageRequest { queueUrl = QUEUE_URL; maxNumberOfMessages = 1 })
					sqsClient.deleteMessage(DeleteMessageRequest { queueUrl = QUEUE_URL; receiptHandle = "r1" })
					sqsClient.receiveMessage(ReceiveMessageRequest { queueUrl = QUEUE_URL; maxNumberOfMessages = 1 })
					sqsClient.deleteMessage(DeleteMessageRequest { queueUrl = QUEUE_URL; receiptHandle = "r2" })
				}
			}
		}

		`when`("A message is not an SNS notification") {
			then("No item is emitted") {
				// Given
				val sqsClient = mockk<SqsClient>()
				val snsSubscriber = mockk<SnsSubscriber>()

				val response1 = ReceiveMessageResponse {
					messages = listOf(Message { receiptHandle = "r1"; body = """{"name":"name1"}""" })
				}

				coEvery { snsSubscriber.subscribe(any(), any()) } returns Result.success(QUEUE_URL)
				coEvery { sqsClient.receiveMessage(any()) } returns response1 andThen ReceiveMessageResponse {}
				coEvery { sqsClient.deleteMessage(any()) } returns DeleteMessageResponse {}
				coEvery { sqsClient.listQueues(any()) } returns ListQueuesResponse { queueUrls = listOf(QUEUE_URL) }

				val sqsConsumer = SqsSnsConsumer(sqsClient, snsSubscriber)

				// When
				val items = runTest(sqsConsumer)

				// Then
				items shouldHaveSize 0
				coVerifyAll {
					sqsClient.receiveMessage(ReceiveMessageRequest { queueUrl = QUEUE_URL; maxNumberOfMessages = 1 })
				}
			}
		}
	}
})

private suspend fun runTest(sqsSnsConsumer: SqsSnsConsumer): MutableList<DomainEvent> {
	val items = mutableListOf<DomainEvent>()
	withTimeoutOrNull(FLOW_TIME_OUT) {
		sqsSnsConsumer.subscribe(QUEUE_NAME, TOPIC, POLL_RATE.milliseconds).toList(items)
	}
	return items
}

private fun makeSnsMessageBody(name: String) = """
	{
	  "Type" : "Notification",
	  "MessageId" : "${UUID.randomUUID()}",
	  "TopicArn" : "arn:aws:sns:eu-central-1:058264299904:tenants",
	  "Message" : 
	  	"{
	  		\"id\": \"123\",
	  		\"type\": \"Update\",
	  		\"time\": \"2024-01-13T17:20:50.161Z\",
	  		\"specversion\": \"1.0.2\",
	  		\"datacontenttype\": \"application/json\",
	  		\"data\":\"{\\\"name\\\": \\\"$name\\\"}\"
	  	}",
	  "Timestamp" : "2024-05-21T13:23:49.581Z",
	  "SignatureVersion" : "1",
	  "Signature" : "Rxo9b4iXCQKPuV8ia1PhNKR",
	  "SigningCertURL" : "https://sns.eu-central-1.amazonaws.com/SimpleNotificationService-60eadc530605d63b8e62a523676ef735.pem",
	  "UnsubscribeURL" : "https://sns.eu-central-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:"
	}
	""".trimIndent()
