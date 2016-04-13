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

public class Topology {
	private static final Logger LOGGER = LoggerFactory.getLogger(Topology.class);
	
	public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();
    String topicName = "distance2"; 	
    BrokerHosts hosts = new ZkHosts("localhost:2181");
    SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, "", "discovery"); 
    spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
    KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
    builder.setSpout("temp", kafkaSpout);
    
    builder.setBolt("control", new MongoInsertBolt(), 3).shuffleGrouping("temp");
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
