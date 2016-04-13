package com.afour.tad.utils;

import java.io.IOException;

import javax.ws.rs.core.MediaType;

import org.eclipse.paho.client.mqttv3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class Publisher {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(Publisher.class);
	private static final String BROKER_URL = "tcp://mqtt.afourtech.com:1883";
	private static final String TOPIC_WATERLEVEL = "distance2";
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
    private void start() throws IOException {
    	try {
    		MqttConnectOptions options = new MqttConnectOptions();
    		options.setCleanSession(false);
			client.connect(options);
			while(true){
			for (int channel_id : channels) {
				Client client1 = Client.create();
	    		WebResource webResource1 = client1
	    				.resource("https://thingspeak.com/channels/" + channel_id + "/feed.json");
	    		LOGGER.info("--> Hit ThingSpeak URI : "+webResource1.getURI());
	    		ClientResponse response1 = webResource1.accept(MediaType.APPLICATION_JSON)
	    				.get(ClientResponse.class);
	    		
	    		if (response1.getStatus() != 200) {
	    			throw new RuntimeException("Failed : HTTP error code : "
	    					+ response1.getStatus());
	    		}
		    	publishWaterLevel(response1.getEntity(String.class));
		        Thread.sleep(50);
    			}
				//run every minutes.
				Thread.sleep(1*60*1000);
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
    public static void main(String args[]) throws IOException  {
        final Publisher publisher = new Publisher();
        publisher.start();
        }
}

