package org.example;

// WeatherDataFetcher.java
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;

public class WeatherDataFetcher {
    private static final String KAFKA_BROKER = "localhost:9092";     // your_kafka_broker_address
    private static final String TOPIC_NAME = "weather-topic";
    private static final String APP_ID = "2a9391878d3c4a87279b49bdc5f73a9d";     // your_openweathermap_api_key

    public static void fetchAndSendWeatherData(String city) throws Exception {
        try {
            // Получение данных о погоде
            JSONObject weatherDataJson = fetchWeatherData(city, APP_ID);

            // Отправка данных в тему Kafka
            Properties properties = new Properties();
            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

            Producer<String, String> producer = new KafkaProducer<>(properties);
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, weatherDataJson.toString());
            producer.send(record);
            producer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static JSONObject fetchWeatherData(String s_city, String appid) throws Exception {
        // Получение данных о погоде
        URL url = new URL("http://api.openweathermap.org/data/2.5/find?q=" + s_city + "&type=like&units=metric&APPID=" + appid);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");

        BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        String inputLine;
        StringBuilder content = new StringBuilder();

        while ((inputLine = in.readLine()) != null) {
            content.append(inputLine);
        }
        in.close();

        // Парсинг JSON-ответа
        return new JSONObject(content.toString());
    }
}

