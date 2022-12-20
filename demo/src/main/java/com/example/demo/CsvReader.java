package com.example.demo;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.json.JSONObject;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.TimeoutException;

import static java.lang.Thread.sleep;

public class CsvReader {

    private static final String QUEUE_NAME="Tudor";
    public void read(){
        try {
            File file = new File("sensor.csv");
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line = " ";
            String[] tempArr;
            int count =0;
            while ((line = br.readLine()) != null) {
                Integer deviceId;
                if(count%2==0) deviceId=4; else deviceId=3;
                tempArr = line.split(",");
                String date= new Timestamp(System.currentTimeMillis()).toString();
                System.out.println(deviceId+" "+tempArr[0] +" "+date);
                JSONObject deviceMessage = new JSONObject();
                deviceMessage.put("deviceId",String.valueOf(deviceId));
                deviceMessage.put("energyConsumption",tempArr[0]);
                deviceMessage.put("date",date);
                count++;
                ConnectionFactory connectionFactory = new ConnectionFactory();
                connectionFactory.setUsername("mnyspvbm");
                connectionFactory.setPassword("QPnkU9RP8ZvX8nieYDRnHHNq_2Ebh2-m");
                connectionFactory.setPort(5672);
                connectionFactory.setUri("amqps://mnyspvbm:QPnkU9RP8ZvX8nieYDRnHHNq_2Ebh2-m@sparrow.rmq.cloudamqp.com/mnyspvbm");

                try (Connection connection = connectionFactory.newConnection();
                     Channel channel = connection.createChannel()) {
                    channel.queueDeclare(QUEUE_NAME, false, false, false, null);
                    channel.basicPublish("", QUEUE_NAME, null, deviceMessage.toString().getBytes(StandardCharsets.UTF_8));
                    System.out.println(" [x] Sent '" + deviceMessage + "'");
                }
                sleep(3000);
            }
            br.close();
        }
        catch(IOException | URISyntaxException | NoSuchAlgorithmException | KeyManagementException | TimeoutException | InterruptedException ioe) {
            ioe.printStackTrace();
        }

    }
}
