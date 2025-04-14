package pl.jacekgajek.sqs.consumer.user

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.mockk.Called
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers.IO
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestScope
import pl.jacekgajek.sqs.consumer.SqsConsumer
import java.time.Instant
import kotlin.time.Duration.Companion.milliseconds

class UserEventConsumerTest : StringSpec() {

	init {

		coroutineTestScope = true

		val queueName = "user-queue"
		val pollRate = 200.milliseconds


		"""Given SQS Consumer emits no items
		When Consumer is started
		Then Tenant consumer isAlive returns true""" {
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
		Then Tenant service is not called""" {

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
		Then Two tenant schema was created""" {

			val sqsConsumer = mockk<SqsConsumer>()
			val userService = mockk<UserEventService>()

			coEvery { userService.onUserEvent(any()) } returns Unit

			every { sqsConsumer.createFlow<UserMessage>(any(), any(), any()) } returns flowOf(
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

			val userEvent1Slot = slot<UserMessage>()
			val userEvent2Slot = slot<UserMessage>()

			coVerify(exactly = 1, timeout = 2000) {
				userService.onUserEvent(capture(userEvent1Slot))
			}

			userEvent1Slot.captured.name shouldBe "John"
			userEvent2Slot.captured.name shouldBe "Mary"
		}
	}
}


private fun createUserCreateEvent(
    name: String,
    created: String,
): UserMessage {
    return UserMessage(
        id = UserId("123"),
        name = name,
        createdAt = Instant.parse(created),
    )
}

