package com.ippontech.kafkatutorials.simpleclient.withavro

import com.github.javafaker.Faker
import com.ippontech.kafkatutorials.simpleclient.Person
import com.ippontech.kafkatutorials.simpleclient.personsAvroTopic
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.LogManager
import java.io.File
import java.util.*

// $ kafka-topics --zookeeper localhost:2181 --create --topic persons-avro --replication-factor 1 --partitions 4

fun main(args: Array<String>) {
    SimpleProducer("localhost:9092", "http://localhost:8081").produce(2)
}

class SimpleProducer(brokers: String, schemaRegistryUrl: String) {

    private val logger = LogManager.getLogger(javaClass)
    private val producer = createProducer(brokers, schemaRegistryUrl)
    private val schema = Schema.Parser().parse(File("src/main/resources/person.avsc"))

    private fun createProducer(brokers: String, schemaRegistryUrl: String): Producer<String, GenericRecord> {
        val props = Properties()
        props["bootstrap.servers"] = brokers
        props["key.serializer"] = StringSerializer::class.java
        props["value.serializer"] = KafkaAvroSerializer::class.java
        props["schema.registry.url"] = schemaRegistryUrl
        return KafkaProducer<String, GenericRecord>(props)
    }

    fun produce(ratePerSecond: Int) {
        val waitTimeBetweenIterationsMs = 1000L / ratePerSecond
        logger.info("Producing $ratePerSecond records per second (1 every ${waitTimeBetweenIterationsMs}ms)")

        val faker = Faker()
        while (true) {
            val fakePerson = Person(
                    firstName = faker.name().firstName(),
                    lastName = faker.name().lastName(),
                    birthDate = faker.date().birthday()
            )
            logger.info("Generated a person: $fakePerson")

            val avroPerson = GenericRecordBuilder(schema).apply {
                set("firstName", fakePerson.firstName)
                set("lastName", fakePerson.lastName)
                set("birthDate", fakePerson.birthDate.time)
            }.build()

            val futureResult = producer.send(ProducerRecord(personsAvroTopic, avroPerson))
            logger.debug("Sent a record")

            Thread.sleep(waitTimeBetweenIterationsMs)

            // wait for the write acknowledgment
            futureResult.get()
        }
    }
}
