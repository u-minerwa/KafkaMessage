package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Random;
import java.util.TimeZone;

public class WeatherDataFetcher2 {
    private static final String KAFKA_BROKER = "localhost:9092"; // your_kafka_broker_address
    private static final String TOPIC_NAME = "weather-topic";
    private static final String APP_ID = "2a9391878d3c4a87279b49bdc5f73a9d"; // your_openweathermap_api_key

    public static void fetchAndSendWeatherData(String city) throws Exception {
        // Получение данных о погоде
        // JSONObject weatherDataJson = fetchWeatherData(city, APP_ID);

        // Отправка данных в тему Kafka
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        try (Producer<String, String> producer = new KafkaProducer<>(properties)) {
//            while (true) {
//                System.out.println("test11");
//                String message = generateTempWeather();
//                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, message);
//                producer.send(record);
//                System.out.println("New message: " + message);
//                Thread.sleep(5000);
//            }

             while (true) {
                 String message = getJson(city);
                 System.out.println("test2");
                 ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, message);
                 producer.send(record);
                 System.out.println(message);
                 Thread.sleep(5000);
             }
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public static String generateTempWeather(){
        return "Temperature: " + (-5 + new Random().nextInt(10));
    }

    public static String getJson(String city) {
        // Получение данных о погоде:
        try {
            // JSONObject data = WeatherDataFetcherFromBefore.fetchWeatherData(s_city, appId);
            JSONObject weatherDataJson = fetchWeatherData(city, APP_ID);

            // Создание JSON формата:
            JSONObject weatherJsonFirst = new JSONObject();
            weatherJsonFirst.put("city", city);
            weatherJsonFirst.put("temp", weatherDataJson.getJSONArray("list").getJSONObject(0).getJSONObject("main").getDouble("temp"));
            weatherJsonFirst.put("temp_min", weatherDataJson.getJSONArray("list").getJSONObject(0).getJSONObject("main").getDouble("temp_min"));
            weatherJsonFirst.put("temp_max", weatherDataJson.getJSONArray("list").getJSONObject(0).getJSONObject("main").getDouble("temp_max"));

            System.out.println("JSON: "+ city + ": " + weatherJsonFirst);

            // Преобразование в строку JSON:
            return weatherJsonFirst.toString();
        } catch (Exception e) {
            System.out.println("Error while fetching data from API");
            return "";
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

