package pl.jacekgajek.sqs.consumer

interface EventConsumer {
	val isActive: Boolean

	fun startConsuming()

	fun stopConsuming()
}
