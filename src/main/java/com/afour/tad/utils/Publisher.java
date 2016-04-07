package com.afour.tad.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import javax.ws.rs.core.MediaType;

import org.eclipse.paho.client.mqttv3.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
public class Publisher {
	public static final String BROKER_URL = "tcp://mqtt.afourtech.com:1883";
	public static final String TOPIC_WATERLEVEL = "distance2";
    private MqttClient client;
    private int[] channels = {91951,102791,104205,104206,105173};
    
    public Publisher() {
    	String clientId = "two" + "-pub";
        try {
            client = new MqttClient(BROKER_URL, clientId);
        } catch (MqttException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
    private void start(String filename) throws IOException {
    	try {
    		MqttConnectOptions options = new MqttConnectOptions();
    		options.setCleanSession(false);
			client.connect(options);
			for (int channel_id : channels) {
				System.out.println(channel_id);
				Client client1 = Client.create();
	    		WebResource webResource1 = client1
	    				.resource("https://thingspeak.com/channels/" + channel_id + "/feed.json");
	    		System.out.println(webResource1.getURI());
	    		ClientResponse response1 = webResource1.accept(MediaType.APPLICATION_JSON)
	    				.get(ClientResponse.class);
	    		
	    		if (response1.getStatus() != 200) {
	    			throw new RuntimeException("Failed : HTTP error code : "
	    					+ response1.getStatus());
	    		}
	    		 //System.out.println(response1.getEntity(String.class));
	    		 //FileWriter file = new FileWriter("D:/StormFarework/TapADrop/src/main/resources/feed_"+channel_id+".json");
	    		 //file.write(response1.getEntity(String.class));
	    		
		    	publishWaterLevel(response1.getEntity(String.class));
		        Thread.sleep(50);
    			}
			}catch (InterruptedException e) {
                e.printStackTrace();
            }catch(MqttException e) {
                e.printStackTrace();
            }
		}
    	
    private void publishWaterLevel(String waterlevel) throws MqttException {
        final MqttTopic waterlevelTopic = client.getTopic(TOPIC_WATERLEVEL);
        waterlevelTopic.publish(new MqttMessage(waterlevel.getBytes()));
    }
    private void publishWaterLevel(JSONObject waterlevel) throws MqttException {
        final MqttTopic waterlevelTopic = client.getTopic(TOPIC_WATERLEVEL);
        MqttMessage message = new MqttMessage();
        message.setQos(0);
        message.setPayload(waterlevel.toString().getBytes());
        System.out.println("message produced : " + message.toString());
        waterlevelTopic.publish(message);
        System.out.println("message sent");
    }

    public static void main(String args[]) throws IOException  {
        final Publisher publisher = new Publisher();
        System.out.println("Published data");
        publisher.start("D:/StormFarework/TapADrop/src/main/resources/water_depth.txt");
        }
}

