package pl.jacekgajek.sqs.consumer

import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.model.DeleteMessageRequest
import aws.sdk.kotlin.services.sqs.model.DeleteMessageResponse
import aws.sdk.kotlin.services.sqs.model.ListQueuesRequest
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
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.serializer
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

private const val URL: String = "queue/url"
private const val QUEUE_NAME: String = "queue-name"
private const val FLOW_TIME_OUT = 50L
private const val POLL_RATE = 5L

class SqsConsumerTest : BehaviorSpec({
	given("External services are mocked") {
		`when`("SQS returns zero items") {
			then("No item is emitted") {
				// Given
				val sqsClient = mockk<SqsClient>()

				val response = ReceiveMessageResponse {
					messages = listOf()
				}

				coEvery { sqsClient.receiveMessage(any()) } returns response andThen ReceiveMessageResponse {}
				coEvery { sqsClient.deleteMessage(any()) } returns DeleteMessageResponse {}
				coEvery { sqsClient.listQueues(any()) } returns ListQueuesResponse { queueUrls = listOf(URL) }

				val sqsConsumer = sqsConsumer(sqsClient)

				// When
				val items = runTest(sqsConsumer)

				// Then
				items shouldHaveSize 0
			}
		}

		`when`("SQS returns two items") {
			// Flaky test
			then("Then those items is emitted").config(enabled = false) {
				// Given
				val sqsClient = mockk<SqsClient>()

				val response1 = ReceiveMessageResponse {
					messages = listOf(Message { receiptHandle = "r1"; body = makeMessageBody("name1") })
				}
				val response2 = ReceiveMessageResponse {
					messages = listOf(Message { receiptHandle = "r2"; body = makeMessageBody("name2") })
				}

				coEvery { sqsClient.receiveMessage(any()) } returns response1 andThen response2 andThen ReceiveMessageResponse {}
				coEvery { sqsClient.deleteMessage(any()) } returns DeleteMessageResponse {}
				coEvery { sqsClient.listQueues(any()) } returns ListQueuesResponse { queueUrls = listOf(URL) }

				val sqsConsumer = sqsConsumer(sqsClient)

				// When
				val items = runTest(sqsConsumer)

				// Then
				items shouldHaveSize 2
				items[0].name shouldBe "name1"
				items[1].name shouldBe "name2"
				coVerifyOrder {
					sqsClient.listQueues(ListQueuesRequest { queueNamePrefix = QUEUE_NAME })
					sqsClient.receiveMessage(ReceiveMessageRequest { queueUrl = URL; maxNumberOfMessages = 1 })
					sqsClient.deleteMessage(DeleteMessageRequest { queueUrl = URL; receiptHandle = "r1" })
					sqsClient.receiveMessage(ReceiveMessageRequest { queueUrl = URL; maxNumberOfMessages = 1 })
					sqsClient.deleteMessage(DeleteMessageRequest { queueUrl = URL; receiptHandle = "r2" })
				}
			}
		}

		`when`("Message is invalid") {
			then("No item is emitted") {
				// Given
				val sqsClient = mockk<SqsClient>()

				val response1 = ReceiveMessageResponse {
					messages = listOf(Message { receiptHandle = "r1"; body = "}{" })
				}

				coEvery { sqsClient.receiveMessage(any()) } returns response1 andThen ReceiveMessageResponse {}
				coEvery { sqsClient.deleteMessage(any()) } returns DeleteMessageResponse {}
				coEvery { sqsClient.listQueues(any()) } returns ListQueuesResponse { queueUrls = listOf(URL) }

				val sqsConsumer = sqsConsumer(sqsClient)

				// When
				val items = runTest(sqsConsumer)

				// Then
				items shouldHaveSize 0
				coVerifyAll {
					sqsClient.listQueues(ListQueuesRequest { queueNamePrefix = QUEUE_NAME })
					sqsClient.receiveMessage(ReceiveMessageRequest { queueUrl = URL; maxNumberOfMessages = 1 })
					sqsClient.receiveMessage(ReceiveMessageRequest { queueUrl = URL; maxNumberOfMessages = 1 })
				}
			}
		}

		`when`("Message does not have a correct schema") {
			then("No item is emitted") {
				// Given
				val sqsClient = mockk<SqsClient>()

				val response1 = ReceiveMessageResponse {
					messages = listOf(Message { receiptHandle = "r1"; body = "{}" })
				}

				coEvery { sqsClient.receiveMessage(any()) } returns response1 andThen ReceiveMessageResponse {}
				coEvery { sqsClient.deleteMessage(any()) } returns DeleteMessageResponse {}
				coEvery { sqsClient.listQueues(any()) } returns ListQueuesResponse { queueUrls = listOf(URL) }

				val sqsConsumer = sqsConsumer(sqsClient)

				// When
				val items = runTest(sqsConsumer)

				// Then
				items shouldHaveSize 0
				coVerifyAll {
					sqsClient.listQueues(ListQueuesRequest { queueNamePrefix = QUEUE_NAME })
					sqsClient.receiveMessage(ReceiveMessageRequest { queueUrl = URL; maxNumberOfMessages = 1 })
				}
			}
		}

		`when`("ReceiveMessage throws exception") {
			then("No item is emitted") {
				// Given
				val sqsClient = mockk<SqsClient>()

				coEvery { sqsClient.receiveMessage(any()) } throws RuntimeException("error") andThen ReceiveMessageResponse {}
				coEvery { sqsClient.deleteMessage(any()) } returns DeleteMessageResponse {}
				coEvery { sqsClient.listQueues(any()) } returns ListQueuesResponse { queueUrls = listOf(URL) }

				val sqsConsumer = sqsConsumer(sqsClient)

				// When
				val items = runTest(sqsConsumer)

				// Then
				items shouldHaveSize 0
				coVerifyAll {
					sqsClient.listQueues(ListQueuesRequest { queueNamePrefix = QUEUE_NAME })
					sqsClient.receiveMessage(ReceiveMessageRequest { queueUrl = URL; maxNumberOfMessages = 1 })
				}
			}

		}
		`when`("DeleteMessage throws exception") {
			then("Item is emitted") {
				// Given
				val sqsClient = mockk<SqsClient>()

				val response1 = ReceiveMessageResponse {
					messages = listOf(Message { receiptHandle = "r1"; body = makeMessageBody("Some") })
				}

				coEvery { sqsClient.receiveMessage(any()) } returns response1 andThen ReceiveMessageResponse {}
				coEvery { sqsClient.deleteMessage(any()) } throws RuntimeException("error")
				coEvery { sqsClient.listQueues(any()) } returns ListQueuesResponse { queueUrls = listOf(URL) }

				val sqsConsumer = sqsConsumer(sqsClient)

				// When
				val items = runTest(sqsConsumer)

				// Then
				items shouldHaveSize 1
				coVerifyAll {
					sqsClient.listQueues(ListQueuesRequest { queueNamePrefix = QUEUE_NAME })
					sqsClient.receiveMessage(ReceiveMessageRequest { queueUrl = URL; maxNumberOfMessages = 1 })
					sqsClient.deleteMessage(DeleteMessageRequest { queueUrl = URL; receiptHandle = "r1" })
				}
			}
		}
	}
})

private fun sqsConsumer(sqsClient: SqsClient) = TestSqsConsumer(sqsClient)

internal class TestSqsConsumer(sqsClient: SqsClient) : SqsConsumer(sqsClient) {
	fun <T : Any> createFlowImpl(
        serializer: KSerializer<T>,
        queueName: String,
        pollRate: Duration,
	): Flow<T> =
		super.createFlow(serializer, queueName, pollRate)
}

private suspend fun runTest(sqsConsumer: TestSqsConsumer): List<SimpleMessage> {
	val items = mutableListOf<SimpleMessage>()
	withTimeoutOrNull(FLOW_TIME_OUT) {
		sqsConsumer.createFlowImpl(serializer<SimpleMessage>(), QUEUE_NAME, POLL_RATE.milliseconds).toList(items)
	}
	return items
}

private fun makeMessageBody(name: String) = """
	{
	  "name": "$name"
	}
	""".trimIndent()

@Serializable
@JvmRecord
private data class SimpleMessage(val name: String)

