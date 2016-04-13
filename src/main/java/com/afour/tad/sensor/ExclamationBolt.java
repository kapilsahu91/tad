package com.afour.tad.sensor;

import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

 public class ExclamationBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	OutputCollector _collector;
	
	@Override
	@SuppressWarnings("rawtypes")
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
    }
    
    @Override
    public void execute(Tuple tuple) {
		try {
			String sentence = tuple.getString(0);
			JSONParser jsonParser = new JSONParser();
			JSONObject jsonObject = (JSONObject) jsonParser.parse(sentence);
			ExclamationTopology.insertData(jsonObject);
			_collector.emit(new Values(jsonObject));
			_collector.ack(tuple);
		}catch(ParseException ex) {
		ex.printStackTrace();
		}catch(Exception ex) {
		ex.printStackTrace();
		}
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("sensor_id","sensor_data"));
    }
  }