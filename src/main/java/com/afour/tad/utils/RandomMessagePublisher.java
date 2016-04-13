package com.afour.tad.utils;

import java.io.IOException;
import java.util.Date;
import org.eclipse.paho.client.mqttv3.*;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RandomMessagePublisher {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(RandomMessagePublisher.class);
	private static final String BROKER_URL = "tcp://mqtt.afourtech.com:1883";
	private static final String TOPIC_WATERLEVEL = "afour_tad_push";
    private MqttClient client;
    
    public RandomMessagePublisher() {
    	String clientId = "two" + "-pub";
        try {
            client = new MqttClient(BROKER_URL, clientId);
        } catch (MqttException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
    @SuppressWarnings("unchecked")
	private void start() throws IOException {
    	LOGGER.info("--> Start sending messages every 5 seconds to mqtt server : " + BROKER_URL
    					+" over topic : " + TOPIC_WATERLEVEL ); 
    	try {
    		MqttConnectOptions options = new MqttConnectOptions();
    		options.setCleanSession(false);
			client.connect(options);
			int i = 0;
			boolean increment = true;
			while(true){
				if(i < 100 && increment){
					i+=5;
				}else if (i == 100){
					i-=5;
					increment = false;
				}else if(i > 0 && !increment)	{
					i-=5;
				}else if(i == 0){
					i+=5;
					increment = true;
				}
				JSONObject json = new JSONObject();
				json.put(Constants.SENSOR_ID,101 );
				json.put(Constants.SENSOR_DATA, i);
				json.put(Constants.TIMESTAMP, new Date().getTime());
				publishWaterLevel(json.toJSONString());
				LOGGER.info("--> Message Emitted " + json);
				json.put(Constants.SENSOR_ID,102 );
				//publishWaterLevel(json.toJSONString());
				LOGGER.info("--> Message Emitted " + json);
				//run every 5 seconds.
				Thread.sleep(5*1000);
			}
			}catch (InterruptedException e) {
                LOGGER.warn(e.getMessage());
            }catch(MqttException e) {
            	LOGGER.warn(e.getMessage());
            }
		}
    	
    private void publishWaterLevel(String waterlevel) throws MqttException {
        final MqttTopic waterlevelTopic = client.getTopic(TOPIC_WATERLEVEL);
        waterlevelTopic.publish(new MqttMessage(waterlevel.getBytes()));
    }
    public static void main(String args[]) throws IOException  {
        final RandomMessagePublisher publisher = new RandomMessagePublisher();
        publisher.start();
        }
}

