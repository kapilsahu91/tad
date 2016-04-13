package com.afour.tad.sensor;

import java.util.Date;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.afour.tad.utils.Constants;
import com.afour.tad.utils.MongoDBConnection;
import com.afour.tad.utils.Utils;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

 public class TADMongoInsertBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	OutputCollector _collector;
	public static DB mongoDb= MongoDBConnection.getMongoInstance().getMongoDb();
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
			insertData(jsonObject);
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
       
    private void insertData(JSONObject json){
		DBObject dbObject = new BasicDBObject();
		dbObject.put(Constants.SEN_ID, json.get("sid"));
		//lets keep the asset ID and sensor ID same as of now.
		dbObject.put(Constants.ASSET_ID, json.get("sid"));
		dbObject.put(Constants.DATA, json.get("dt"));
		dbObject.put(Constants.DATA_DATE, Utils.getDateStringInISO(new Date((long) json.get("ts"))));	
		dbObject.put(Constants.CREATED_DATE,Utils.getDateStringInISO(new Date()));
		dbObject.put(Constants.MODIFY_DATE,Utils.getDateStringInISO(new Date()));
		dbObject.put(Constants.APP_ID,1);
		mongoDb.getCollection("IOT_TAD_ASSET_DATA").save(dbObject);					
		}
  }