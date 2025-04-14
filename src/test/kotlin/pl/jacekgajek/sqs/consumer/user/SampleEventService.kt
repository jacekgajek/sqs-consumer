package pl.jacekgajek.sqs.consumer.user

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.Serializer
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import java.time.Instant

interface UserEventService {
	suspend fun onUserEvent(message: UserMessage)
}

@JvmInline
@Serializable
value class UserId(val id: String)

@Serializable
data class UserMessage(
    val id: UserId,
    val name: String,
    @Serializable(with = InstantSerializer::class)
    val createdAt: Instant
    )

@OptIn(ExperimentalSerializationApi::class)
@Serializer(forClass = Instant::class)
object InstantSerializer : KSerializer<Instant> {
    override fun deserialize(decoder: Decoder): Instant
        = Instant.parse(decoder.decodeString())

    override fun serialize(encoder: Encoder, value: Instant) {
        encoder.encodeString(value.toString())
    }
}