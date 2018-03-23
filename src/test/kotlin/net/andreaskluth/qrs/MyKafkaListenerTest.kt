package net.andreaskluth.qrs

import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit4.SpringRunner
import java.util.UUID

@RunWith(SpringRunner::class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@EmbeddedKafka(partitions = 1, topics = ["myTopic"])
@DirtiesContext
class MyKafkaListenerTest {

    @Autowired
    lateinit var template : KafkaTemplate<String, String>

    @Autowired
    lateinit var myKafkaListener : MyKafkaListener

    @Test
    fun happyPathSuccess() {
        template.send("myTopic", UUID.randomUUID().toString(), "value")
        myKafkaListener.latch.await()
    }

}