package io.github.jacekgajek.sqs.consumer

interface EventConsumer {
	val isActive: Boolean

	fun startConsuming()

	fun stopConsuming()
}
