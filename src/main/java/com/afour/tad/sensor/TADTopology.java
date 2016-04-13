package com.afour.tad.sensor;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.spout.SchemeAsMultiScheme;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.afour.tad.bolts.MetricsProcessingBolt;

public class TADTopology {
	private static final Logger LOGGER = LoggerFactory.getLogger(TADTopology.class);
	
	public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();
    String topicName = "afour_tad_push"; 	
    BrokerHosts hosts = new ZkHosts("localhost:2181");
    
    SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, "", "discovery"); 
    spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
    KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
    builder.setSpout("temp", kafkaSpout);
    
    builder.setBolt("control", new TADMongoInsertBolt(), 3).shuffleGrouping("temp");
    builder.setBolt("controlMetrics", new MetricsProcessingBolt("IOT_TAD_METRIC_TREND"), 3).shuffleGrouping("temp");
    
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
