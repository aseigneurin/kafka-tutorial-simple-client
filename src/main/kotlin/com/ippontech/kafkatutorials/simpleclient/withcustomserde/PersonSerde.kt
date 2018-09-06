package com.ippontech.kafkatutorials.simpleclient.withcustomserde

import com.ippontech.kafkatutorials.simpleclient.Person
import com.ippontech.kafkatutorials.simpleclient.jsonMapper
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer

class PersonSerializer : Serializer<Person> {
    override fun serialize(topic: String, data: Person?): ByteArray? {
        if (data == null) return null
        return jsonMapper.writeValueAsBytes(data)
    }

    override fun close() {}
    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {}
}

class PersonDeserializer : Deserializer<Person> {
    override fun deserialize(topic: String, data: ByteArray?): Person? {
        if (data == null) return null
        return jsonMapper.readValue(data, Person::class.java)
    }

    override fun close() {}
    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {}
}
