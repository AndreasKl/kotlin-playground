package net.andreaskluth.qrs

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.listener.AbstractMessageListenerContainer.AckMode
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component
import java.util.concurrent.CountDownLatch


@SpringBootApplication
class QuantumRedShiftConfig constructor(var consumerFactory: ConsumerFactory<String?, String?>) {
    @Bean
    fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String?, String?> {
        val factory: ConcurrentKafkaListenerContainerFactory<String?, String?> = ConcurrentKafkaListenerContainerFactory()
        factory.consumerFactory = consumerFactory
        factory.containerProperties.ackMode = AckMode.MANUAL
        return factory
    }
}

@Component
class MyKafkaListener {

    companion object {
        val log: Logger = LoggerFactory.getLogger(this::class.java)
    }

    val latch: CountDownLatch = CountDownLatch(1)

    @KafkaListener(topics = ["myTopic"])
    fun listen(message: String, ack: Acknowledgment) {
        log.info("Received message with payload: {}", message)
        ack.acknowledge()
        latch.countDown()
    }

}

fun main(args: Array<String>) {
    runApplication<QuantumRedShiftConfig>(*args)
}