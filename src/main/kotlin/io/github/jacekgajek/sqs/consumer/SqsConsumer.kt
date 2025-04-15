package io.github.jacekgajek.sqs.consumer

import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.model.DeleteMessageRequest
import aws.sdk.kotlin.services.sqs.model.GetQueueUrlRequest
import aws.sdk.kotlin.services.sqs.model.Message
import aws.sdk.kotlin.services.sqs.model.ReceiveMessageRequest
import com.sksamuel.tabby.results.catching
import com.sksamuel.tabby.results.failureIfNull
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.isActive
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import kotlin.time.Duration

/**
 * Consumes messages from an SQS queue and emits them as a Kotlin flow.
 */
open class SqsConsumer(private val sqsClient: SqsClient) {
    private val log = KotlinLogging.logger { }

    /**
     * Creates a flow that continuously polls messages from the specified SQS queue, deserializes them using
     * the provided serializer, and emits the messages for processing. The flow operates as long as the coroutine
     * context is active and processes messages at the defined polling rate.
     *
     * If the current coroutine context is cancelled, the flow closes only after the last message in a batch is processed.
     *
     * @param T The type of the messages being processed.
     * @param serializer The serializer used to deserialize messages from the queue.
     * @param queueName The name of the SQS queue to poll messages from.
     * @param pollRate The interval at which the queue is polled for messages.
     * @param maxNumberOfMessagesInBatch The maximum number of messages to retrieve in a single poll (default is 10).
     * @return A flow emitting deserialized messages from the queue.
     * @throws QueueNotFoundException When the specified queue cannot be found.
     */
    @Throws(QueueNotFoundException::class)
    fun <T : Any> createFlow(
        serializer: KSerializer<T>,
        queueName: String,
        pollRate: Duration,
        maxNumberOfMessagesInBatch: Int = 10,
    ): Flow<T> =
        SqsConsumerImpl(serializer, queueName, pollRate, maxNumberOfMessagesInBatch).createFlow()

    private inner class SqsConsumerImpl<T : Any>(
        private val serializer: KSerializer<T>,
        private val queueName: String,
        private val pollRate: Duration,
        private val maxNumberOfMessagesInBatch: Int,
    ) {
        private var cachedQueueUrl: String? = null

        fun createFlow() = flow {
            while (currentCoroutineContext().isActive) {
                getQueueUrl().map { queueUrl ->
                    consumeMessage(queueUrl).onSuccess { response ->
                        response.messages?.processMessages { receiptHandle, body ->
                            decode(body).onSuccess { message ->
                                log.trace { "Successfully received and parsed a message of type [${message::class.qualifiedName}]." }
                                emit(message)
                                log.debug { "Processing a message of type [${message::class.qualifiedName}] completed without exception. Deleting it from the queue [$queueName]." }
                                deleteMessage(queueUrl, receiptHandle)
                            }
                        }
                    }
                }
                delay(pollRate)
            }
        }

        private suspend fun List<Message>.processMessages(consumer: suspend (receiptHandle: String, body: String) -> Unit) =
            this.filter { it.receiptHandle != null && it.body != null }
                .forEach { consumer(it.receiptHandle!!, it.body!!) }

        private suspend fun getQueueUrl(): Result<String> =
            cachedQueueUrl.failureIfNull { QueueNotFoundException("Cannot get queue url for queue $queueName.") }
                .coFlatRecover { fetchQueueUrl() }

        private suspend fun fetchQueueUrl(): Result<String> =
            sqsClient.getQueueUrl(GetQueueUrlRequest { queueName = this@SqsConsumerImpl.queueName })
                .queueUrl
                .failureIfNull()

        private suspend fun deleteMessage(queueUrlArg: String, messageReceiptHandle: String) = catching {
            sqsClient.deleteMessage(DeleteMessageRequest {
                queueUrl = queueUrlArg
                receiptHandle = messageReceiptHandle
            }).also { log.trace { "Successfully deleted a message $messageReceiptHandle from queue $queueUrlArg: $it" } }
        }.onFailure { log.error(it) { "Error during deleting a message with handle '$messageReceiptHandle': ${it.message}" } }

        private suspend fun consumeMessage(queueUrlArg: String) = catching {
            sqsClient.receiveMessage(ReceiveMessageRequest {
                queueUrl = queueUrlArg
                maxNumberOfMessages = maxNumberOfMessagesInBatch
            })
        }.onFailure { log.error(it) { "Error during receiving a message from $queueUrlArg: ${it.message}" } }

        private fun decode(messageBody: String): Result<T> = catching {
            Json.decodeFromString(serializer, messageBody)
                .also { log.trace { "Successfully decoded a message from queue $messageBody: $it" } }
        }.onFailure { log.error(it) { "Cannot deserialize a message with $serializer: $messageBody" } }
    }

    /**
     * If this [Result] is a failure, maps it to result of [block] method and unwraps it from a nested [Result].
     * If this [Result] is a success, returns the same success.
     * The [block] must have the same result type as this [Result].
     *
     * @param block The block to apply to the thrown exception.
     * @return The result of applying the block to the exception or the same success.
     */
    private suspend fun <T> Result<T>.coFlatRecover(block: suspend (Throwable) -> Result<T>): Result<T> =
        when {
            this.isFailure -> block(this.exceptionOrNull()!!)
            else -> this
        }
}

/**
 * Exception thrown when the provided SQS queue cannot be found.
 *
 * This exception is raised when attempting to retrieve the URL
 * of an SQS queue, but the queue name does not resolve to a valid queue
 * within AWS SQS. It serves as an indicator of an operational issue where
 * the queue may not exist, is misconfigured, or is inaccessible.
 *
 * @param queue The name of the SQS queue that could not be found.
 */
class QueueNotFoundException(queue: String) : RuntimeException("Could not find queue: [$queue]")
