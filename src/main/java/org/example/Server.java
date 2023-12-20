package org.example;

// Server.java
public class Server {
    public static void mainServer() {
        try {
            WeatherDataFetcher2.fetchAndSendWeatherData("Saratov");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

