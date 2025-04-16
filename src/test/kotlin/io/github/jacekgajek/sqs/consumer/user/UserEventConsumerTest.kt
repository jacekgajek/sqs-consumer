package io.github.jacekgajek.sqs.consumer.user

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.mockk.Called
import io.mockk.coVerify
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers.IO
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestScope
import io.github.jacekgajek.sqs.consumer.SqsConsumer
import kotlinx.coroutines.delay
import kotlinx.serialization.json.Json
import java.time.Instant
import kotlin.time.Duration.Companion.milliseconds

class UserEventConsumerTest : StringSpec() {

	init {


		val queueName = "user-queue"
		val pollRate = 200.milliseconds


		"""Given SQS Consumer emits no items
		When Consumer is started
		Then consumer isAlive returns true""" {
			val sqsConsumer = mockk<SqsConsumer>()
			val userService = mockk<UserEventService>()

			val consumer = SampleEventConsumer.of(
				sqsConsumer,
				queueName,
				pollRate,
				userService,
				TestScope(StandardTestDispatcher())
			)
			consumer.startConsuming()

			consumer.isActive shouldBe true
		}

		"""Given SQS Consumer emits no items
		When Consumer is started
		Then service is not called""" {

			val sqsConsumer = mockk<SqsConsumer>()
			val userService = mockk<UserEventService>()

			val consumer = SampleEventConsumer.of(
				sqsConsumer,
				queueName,
				pollRate,
				userService,
				TestScope(StandardTestDispatcher())
			)
			consumer.startConsuming()

			verify { userService wasNot Called }
		}


        """Given SQS Consumer emits two items
		When Consumer is started
		Then Two items are received""" {

            val sqsConsumer = mockk<SqsConsumer>()
            val userService = spyk<UserEventService>(TestUserEventService())

            every { sqsConsumer.createFlow(any(), any(), any()) } returns flowOf(
                createUserCreateEvent("John", "2025-01-01T00:00:00Z"),
                createUserCreateEvent("Mary", "2025-01-01T01:00:00Z"),
            )

            val consumer = SampleEventConsumer.of(
                sqsConsumer,
                queueName,
                pollRate,
                userService,
                CoroutineScope(IO),
            )

            consumer.startConsuming()

            val userEventSlots = mutableListOf<UserMessage>()

            coVerify(exactly = 2, timeout = 2000) {
                userService.onUserEvent(capture(userEventSlots))
            }

            userEventSlots shouldHaveSize 3
            userEventSlots[1].name shouldBe "John"
            userEventSlots[2].name shouldBe "Mary"
        }

		"""Given SQS Consumer emits invalid item
		When Consumer is started
		Then no message is received""" {

			val sqsConsumer = mockk<SqsConsumer>()
			val userService = spyk<UserEventService>(TestUserEventService())

			every { sqsConsumer.createFlow(any(), any(), any()) } returns flowOf(
				"}invalid message{"
			)

			val consumer = SampleEventConsumer.of(
				sqsConsumer,
				queueName,
				pollRate,
				userService,
                CoroutineScope(IO),
			)

			consumer.startConsuming()

			coVerify(exactly = 0, timeout = 2000) {
				userService.onUserEvent(any())
			}
            delay(1000)
		}
	}
}

private class TestUserEventService : UserEventService {
	override suspend fun onUserEvent(message: UserMessage) {
        println("onUserEvent: $message")
	}
}


private fun createUserCreateEvent(
    name: String,
    created: String,
): String {
    return Json.encodeToString(UserMessage(
        id = UserId("123"),
        name = name,
        createdAt = Instant.parse(created),
    ))
}

