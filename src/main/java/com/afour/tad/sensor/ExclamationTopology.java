package com.afour.tad.sensor;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.spout.SchemeAsMultiScheme;

import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExclamationTopology {
	private static final Logger LOGGER = LoggerFactory.getLogger(ExclamationTopology.class);
	
	
	@SuppressWarnings("unchecked")
	public static void insertData(JSONObject jsonObject1){
	  //-- new configs appended to take up channel details starts
	  JSONObject channnelJson= (JSONObject)jsonObject1.get("channel");
	  
	  Client client1 = Client.create();
	  WebResource webResource1 = client1
			  .resource("http://localhost:8080/TestWebApp/rest/mongo/insertChannel");
	  LOGGER.info("--> POSTing channel information : " + channnelJson);
	  
	  ClientResponse response1 = webResource1.type(MediaType.APPLICATION_JSON)
			  .post(ClientResponse.class,channnelJson.toJSONString());
	  
	  if (response1.getStatus() != 200) {
		  throw new RuntimeException("Failed : HTTP error code : "
				  + response1.getStatus());
	  }
	  //send all feeds in json format
	  long id = (long)(channnelJson.get("id"));	  
	  JSONArray feedsArray= (JSONArray)jsonObject1.get("feeds");
      for (int i = 0 ; i < feedsArray.size(); i++) {
          JSONObject jsonObject = (JSONObject)feedsArray.get(i);
          jsonObject.put("channel_id", id);  
		  try{
		  	Client client = Client.create();
			WebResource webResource = client
			   .resource("http://localhost:8080/TestWebApp/rest/mongo/insertSensor");
			ClientResponse response = webResource.type(MediaType.APPLICATION_JSON)
					.post(ClientResponse.class,jsonObject.toJSONString());
			
			LOGGER.info("--> POSTing sensor's data with entryId  : " + jsonObject.get("entry_id") +
					" for channel : " + id);
	
			if (response.getStatus() != 200) {
			   throw new RuntimeException("Failed : HTTP error code : "
				+ response.getStatus());
			}
		  }catch(Exception ex){
			  ex.printStackTrace();
		  }
      }
  }
  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();
    String topicName = "distance2"; 	
    BrokerHosts hosts = new ZkHosts("localhost:2181");
    SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, "", "discovery"); 
    spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
    KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
    builder.setSpout("temp", kafkaSpout);
    builder.setBolt("control", new ExclamationBolt(), 3).shuffleGrouping("temp");
    Config conf = new Config();
    conf.setDebug(true);
    if (args != null && args.length > 0) {
       conf.setNumWorkers(3);
      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }
    else {
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("test", conf, builder.createTopology());
      Utils.sleep(10000000);
      cluster.killTopology("test");
      cluster.shutdown();
    }
  }
}
