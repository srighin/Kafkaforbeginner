import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {

    private static Logger logger = LoggerFactory.getLogger("Producer");

    public static void main(String[] args) {

        //Create properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //Create producer record
        for(int i=0; i<10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "_id:"+i,"Hello World "+i);

            //send message | Asynchronously
            producer.send(record, new Callback(){

                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // Execute every time a message is sent successfully, or throws exception in case of error.
                    if(e == null){
                        logger.info("Offset : {}", recordMetadata.offset());
                        logger.info("partition {}: ", recordMetadata.partition());
                        logger.info("topic : {}", recordMetadata.topic());
                        logger.info("timestamp : {}", recordMetadata.timestamp());
                    } else {
                        logger.info(e.getMessage());
                    }

                }
            });
        }


        //flush data
        producer.flush();
        producer.close();


    }
}
