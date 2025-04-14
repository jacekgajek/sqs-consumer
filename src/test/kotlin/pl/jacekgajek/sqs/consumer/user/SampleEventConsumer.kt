package pl.jacekgajek.sqs.consumer.user

import kotlinx.coroutines.CoroutineScope
import pl.jacekgajek.sqs.consumer.BaseEventConsumer
import pl.jacekgajek.sqs.consumer.SqsConsumer
import kotlin.reflect.KClass
import kotlin.time.Duration

class SampleEventConsumer
private constructor(
    consumer: SqsConsumer,
    queueName: String,
    pollRate: Duration,
    coroutineScope: CoroutineScope,
    dataClazz: KClass<UserMessage>,
    private val userEventService: UserEventService,
) : BaseEventConsumer<UserMessage>(
    consumer,
    queueName,
    pollRate,
    coroutineScope,
    dataClazz,
) {

    companion object {
        internal fun of(
            consumer: SqsConsumer,
            queueName: String,
            pollRate: Duration,
            sampleEventService: UserEventService,
            scope: CoroutineScope,
        ): SampleEventConsumer = SampleEventConsumer(
            consumer,
            queueName,
            pollRate,
            scope,
            UserMessage::class,
            sampleEventService,
        )
    }

    override suspend fun processMessage(message: UserMessage) {
        userEventService.onUserEvent(message)
    }
}