package pl.jacekgajek.sqs.consumer

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

    fun <T : Any> createFlow(
        serializer: KSerializer<T>,
        queueName: String,
        pollRate: Duration,
    ): Flow<T> =
        SqsConsumerImpl(serializer, queueName, pollRate).createFlow()

    protected inner class SqsConsumerImpl<T : Any>(
        private val serializer: KSerializer<T>,
        private val queueName: String,
        private val pollRate: Duration,
    ) {
        private var cachedQueueUrl: String? = null

        fun createFlow() = flow {
            while (currentCoroutineContext().isActive) {
                getQueueUrl().map { queueUrl ->
                    consumeMessage(queueUrl).onSuccess { response ->
                        response.messages?.processSingleMessage { receiptHandle, body ->
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

        private suspend fun List<Message>.processSingleMessage(consumer: suspend (receiptHandle: String, body: String) -> Unit) =
            this.singleOrNull()
                ?.takeIf { it.receiptHandle != null && it.body != null }
                ?.let { consumer(it.receiptHandle!!, it.body!!) }

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
                maxNumberOfMessages = 1
            })
        }.onFailure { log.error(it) { "Error during receiving a message from $queueUrlArg: ${it.message}" } }

        private fun decode(messageBody: String): Result<T> = catching {
            Json.decodeFromString(serializer, processRawBody(messageBody))
                .also { log.trace { "Successfully decoded a message from queue $messageBody: $it" } }
        }.onFailure { log.error(it) { "Cannot deserialize a message with $serializer: $messageBody" } }
    }

    /**
     * Additional operation to perform on the message body before deserializing it into the target type.
     *
     * @param messageBody The raw message body to be processed.
     * @return The processed message body ready for deserialization.
     */
    open fun processRawBody(messageBody: String): String = messageBody

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

class QueueNotFoundException(queue: String) : Exception("Could not find queue: [$queue]")
