package org.example;


public class Main {
    public static void main(String[] args) throws Exception {
        // System.out.println("Send message:");
        //Server.mainServer();
        //WeatherDataFetcher2.getJson("Saratov");

        WeatherDataFetcher2.fetchAndSendWeatherData("Saratov");
    }
}

