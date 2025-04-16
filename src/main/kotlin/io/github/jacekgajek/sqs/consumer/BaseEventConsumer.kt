package io.github.jacekgajek.sqs.consumer

import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.retry
import kotlinx.coroutines.launch
import kotlinx.serialization.KSerializer
import kotlinx.serialization.serializer
import kotlin.reflect.KClass
import kotlin.reflect.full.createType
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

private val log = KotlinLogging.logger { }

abstract class BaseEventConsumer<T : Any> (
    private val consumer: SqsConsumer,
    private val queueName: String,
    private val pollRate: Duration,
    private val coroutineScope: CoroutineScope,
    private val dataClazz: KClass<T>,
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

			consumer.createFlow(defaultSerializer(), queueName, pollRate)
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
                    true
				}
				.collect()
		}
	}

	@Suppress("UNCHECKED_CAST")
	private fun defaultSerializer() = serializer(dataClazz.createType()) as KSerializer<T>

	abstract suspend fun processMessage(event: T)
}
