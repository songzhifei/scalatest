package day01;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

public class KafkaOperator {
    public static void main( String[] args )
    {
        //System.out.println( "Hello World!" );
        Properties props = new Properties();
        props.put("bootstrap.servers", "itcast02:9092,itcast03:9092");
        props.put("group.id", "CountryCounter");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String,String>(props);

        consumer.subscribe(Collections.singletonList("customerCountries"));
        try {
            while (true) {  //1)
                ConsumerRecords<String, String> records = consumer.poll(100);  //2)
                for (ConsumerRecord<String, String> record : records)  //3)
                {
                    System.out.println(record.value());
//                    List<HashMap> custCountryMap = new List<HashMap>();
//                    int updatedCount = 1;
//                    if (custCountryMap.contains(record.value())) {
//                        updatedCount = custCountryMap.get(record.value()) + 1;
//                    }
//                    custCountryMap.put(record.value(), updatedCount);
//                    JSONObject json = new JSONObject(custCountryMap);
//                    System.out.println(json.toString(4));
                }
            }
        } finally {
            consumer.close(); //4
        }

    }
}
