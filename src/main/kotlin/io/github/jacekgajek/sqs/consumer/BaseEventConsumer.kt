package io.github.jacekgajek.sqs.consumer

import com.sksamuel.tabby.results.catching
import com.sksamuel.tabby.results.mapError
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.retry
import kotlinx.coroutines.launch
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.serializer
import kotlin.reflect.KClass
import kotlin.reflect.full.createType
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

private val log = KotlinLogging.logger { }

abstract class BaseEventConsumer<T : Any>(
    private val consumer: SqsConsumer,
    private val queueName: String,
    private val pollRate: Duration,
    private val coroutineScope: CoroutineScope,
    private val dataClazz: KClass<T>,
    private val maxNumberOfMessagesInBatch: Int = 1,
) : EventConsumer {
    private val consumerName = this::class.simpleName
    private var job: Job? = null

    override val isActive
        get() = job?.isActive == true

    override fun stopConsuming() {
        if (isActive) {
            job?.cancel("$consumerName stop() method was called.")
        }
    }

    override fun startConsuming() {
        job = coroutineScope.launch {
            log.info { "Starting the $consumerName." }

            val defaultSerializer = defaultSerializer()
            consumer.createFlow(queueName, pollRate, maxNumberOfMessagesInBatch)
                .map { decode(it, customSerializer(it, defaultSerializer)) }
                .map { it.getOrThrow() }
                .onEach { event ->
                    log.trace { "Received ${dataClazz.simpleName} event." }
                    processMessage(event)
                    log.trace { "Finished processing ${dataClazz.simpleName} event." }
                }
                .retry { ex ->
                    if (ex is QueueNotFoundException) {
                        log.error { "Cannot start consumer: queue [$queueName] was not found. Retrying in 10 seconds..." }
                        delay(10.seconds)
                    } else {
                        log.error(ex) { "Exception during processing a message in queue [$queueName]. The message won't be deleted from the queue. [${ex.message}]." }
                    }
                    delay(pollRate)
                    true
                }
                .collect()
        }
    }

    private fun decode(messageBody: String, serializer: KSerializer<T>): Result<T> = catching {
        Json.decodeFromString(serializer, messageBody)
            .also { log.trace { "Successfully decoded a message from queue [$messageBody]: [$it]" } }
    }.onFailure { log.error(it) { "Cannot deserialize a message with [$serializer]: [$messageBody]" } }
        .mapError { MessageDeserializationFailedException(messageBody, queueName, it) }

    /**
     * Retrieves the default serializer for the generic type [T] of the event being consumed.
     *
     * This method will be called only once while creating a flow. To customize serializer per message, override the customSerializer method
     *
     * @return The serializer of type [KSerializer] for the generic type [T].
     */
    @Suppress("UNCHECKED_CAST")
    open fun defaultSerializer() = serializer(dataClazz.createType()) as KSerializer<T>

    /**
     * Allows customization of the serializer to be used for deserializing the message.
     * This method can be overridden to provide a custom serializer for individual messages
     * based on message content or other contextual information.
     *
     * @param message The raw message string to be deserialized.
     * @param defaultSerializer The default serializer for the generic type [T].
     *        This is the serializer returned by `defaultSerializer()` method.
     * @return The serializer to be used for the given [message].
     */
    open fun customSerializer(message: String, defaultSerializer: KSerializer<T>) = defaultSerializer

    abstract suspend fun processMessage(event: T)
}

class MessageDeserializationFailedException(message: String, queue: String, cause: Throwable) :
    RuntimeException("Deserialization of message from queue [$queue] failed: [$message]", cause)
