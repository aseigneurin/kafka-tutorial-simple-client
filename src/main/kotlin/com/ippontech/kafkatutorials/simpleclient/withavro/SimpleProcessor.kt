package com.ippontech.kafkatutorials.simpleclient.withavro

import com.ippontech.kafkatutorials.simpleclient.Person
import com.ippontech.kafkatutorials.simpleclient.agesTopic
import com.ippontech.kafkatutorials.simpleclient.personsAvroTopic
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.LogManager
import java.time.Duration
import java.time.LocalDate
import java.time.Period
import java.time.ZoneId
import java.util.*

// $ kafka-topics --zookeeper localhost:2181 --create --topic ages --replication-factor 1 --partitions 4

fun main(args: Array<String>) {
    SimpleProcessor("localhost:9092", "http://localhost:8081").process()
}

class SimpleProcessor(brokers: String, schemaRegistryUrl: String) {

    private val logger = LogManager.getLogger(javaClass)
    private val consumer = createConsumer(brokers, schemaRegistryUrl)
    private val producer = createProducer(brokers)

    private fun createConsumer(brokers: String, schemaRegistryUrl: String): Consumer<String, GenericRecord> {
        val props = Properties()
        props["bootstrap.servers"] = brokers
        props["group.id"] = "person-processor"
        props["key.deserializer"] = StringDeserializer::class.java
        props["value.deserializer"] = KafkaAvroDeserializer::class.java
        props["schema.registry.url"] = schemaRegistryUrl
        return KafkaConsumer<String, GenericRecord>(props)
    }

    private fun createProducer(brokers: String): Producer<String, String> {
        val props = Properties()
        props["bootstrap.servers"] = brokers
        props["key.serializer"] = StringSerializer::class.java
        props["value.serializer"] = StringSerializer::class.java
        return KafkaProducer<String, String>(props)
    }

    fun process() {
        consumer.subscribe(listOf(personsAvroTopic))

        logger.info("Consuming and processing data")

        while (true) {
            val records = consumer.poll(Duration.ofSeconds(1))
            logger.info("Received ${records.count()} records")

            records.iterator().forEach {
                val personAvro = it.value()

                val person = Person(
                        firstName = personAvro["firstName"].toString(),
                        lastName = personAvro["lastName"].toString(),
                        birthDate = Date(personAvro["birthDate"] as Long)
                )
                logger.debug("Person: $person")

                val birthDateLocal = person.birthDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate()
                val age = Period.between(birthDateLocal, LocalDate.now()).getYears()
                logger.debug("Age: $age")

                val future = producer.send(ProducerRecord(agesTopic, "${person.firstName} ${person.lastName}", "$age"))
                future.get()
            }
        }
    }
}
