package com.ippontech.kafkatutorials.simpleclient.computelatency

import com.ippontech.kafkatutorials.simpleclient.Person
import com.ippontech.kafkatutorials.simpleclient.withcustomserde.PersonDeserializer
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.LogManager
import java.time.Duration
import java.util.*

fun main(args: Array<String>) {
    val latencyAnalyzer = LatencyAnalyzer("localhost:9092", "persons", "ages")
    latencyAnalyzer.start()
}

class LatencyAnalyzer(val brokers: String, val inputTopic: String, val outputTopic: String) {

    private val logger = LogManager.getLogger(javaClass)
    private val inputConsumer = createInputConsumer()
    private val outputConsumer = createOutputConsumer()
    private val inputTimestampMap = mutableMapOf<String, Long>()

    private fun createInputConsumer(): Consumer<String, Person> {
        val props = Properties()
        props["bootstrap.servers"] = brokers
        props["group.id"] = "latency-analyzer-7"
        props["key.deserializer"] = StringDeserializer::class.java
        props["value.deserializer"] = PersonDeserializer::class.java
        props["fetch.min.bytes"] = 10000
        return KafkaConsumer<String, Person>(props)
    }

    private fun createOutputConsumer(): Consumer<String, String> {
        val props = Properties()
        props["bootstrap.servers"] = brokers
        props["group.id"] = "latency-analyzer-7"
        props["key.deserializer"] = StringDeserializer::class.java
        props["value.deserializer"] = StringDeserializer::class.java
        return KafkaConsumer<String, String>(props)
    }

    fun start() {
        inputConsumer.subscribe(listOf(inputTopic))
        outputConsumer.subscribe(listOf(outputTopic))

        while (true) {
            readFromInputTopic()
            readFromOutputTopic()
        }
    }

    private fun readFromInputTopic() {
        val records = inputConsumer.poll(Duration.ofSeconds(1))
        logger.debug("Received ${records.count()} records from the input topic")
        records.iterator().forEach {
            val person = it.value()
            val key = "${person.firstName} ${person.lastName}"
            val timestamp = it.timestamp()
            inputTimestampMap.put(key, timestamp)
        }
    }

    private fun readFromOutputTopic() {
        val records = outputConsumer.poll(Duration.ofSeconds(1))
        logger.debug("Received ${records.count()} records from the output topic")
        records.iterator().forEach {
            val key = it.key()
            val timestamp = inputTimestampMap[key]
            if (timestamp == null) {
                println("Key '$key' not found in timestamp map")
            } else {
                val latency = it.timestamp() - timestamp
                println("Latency: $latency")
            }
        }
    }
}