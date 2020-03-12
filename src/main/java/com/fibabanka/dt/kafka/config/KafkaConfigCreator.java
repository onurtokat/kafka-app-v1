package com.fibabanka.dt.kafka.config;

import com.fibabanka.dt.kafka.util.Constants;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;

import static com.fibabanka.dt.kafka.util.Constants.DEV_ENVIRONMENT;

/**
 * @author OnurTokat
 * KafkaConfigCreator class creates Consumer config
 */
public class KafkaConfigCreator {

    private static String bootStrapServers = null;
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConfigCreator.class);

    public static Properties getConfig(String environmentType, boolean offsetReset) {

        LOGGER.info("ConfigCreator getConfig method has been invoked with parameter: " + environmentType);

        Properties config = new Properties();

        if (environmentType.toLowerCase().equals(DEV_ENVIRONMENT)) {
            bootStrapServers = Constants.DEV_BOOTSTRAP_SERVERS;
        } else {
            bootStrapServers = Constants.PROD_BOOTSTRAP_SERVERS;
        }

        LOGGER.info("Bootstrap servers: " + bootStrapServers);

        //according to offset reset, topic is consumed from beginning or rest
        if (offsetReset) {
            config.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-consumer-app-group_" + UUID.randomUUID());//"kafka-consumer-app-group"
            config.put(ConsumerConfig.CLIENT_ID_CONFIG, "kafka-consumer-app-client_" + UUID.randomUUID());//"kafka-consumer-app-client"
            config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
            config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        } else {
            config.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-consumer-app-group");//"kafka-consumer-app-group"
            config.put(ConsumerConfig.CLIENT_ID_CONFIG, "kafka-consumer-app-client");//"kafka-consumer-app-client"
            config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
            config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        }
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        config.put("sasl.kerberos.service.name", "kafka");
        config.put("sasl.mechanism", "GSSAPI");

        return config;
    }
}