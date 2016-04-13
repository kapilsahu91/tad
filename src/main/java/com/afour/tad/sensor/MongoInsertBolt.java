package com.afour.tad.sensor;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

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

 public class MongoInsertBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	OutputCollector _collector;
	private static Map<String, HashMap<String, String>> sensorChannel = com.afour.tad.utils.Utils.sensorChannel;
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
    	JSONObject channel = (JSONObject) json.get("channel");
    	String channelId = channel.get("id").toString();
    	JSONArray allFeeds = (JSONArray) json.get("feeds");
    		
    	for (int i = 0 ; i < allFeeds.size() ; i++){
    		Set<String> keys = ((JSONObject)allFeeds.get(i)).keySet();
    		for (String key : keys) {
				if(key.contains("field") &&  ((JSONObject)allFeeds.get(i)).get(key) != null){
					String sensorId = sensorChannel.get(channelId).get(key);
					String data = ((JSONObject)allFeeds.get(i)).get(key).toString();
					
					DBObject dbObject = new BasicDBObject();
					dbObject.put("SEN_ID", sensorId);
					//lets keep the asset ID and sensor ID same as of now.
					dbObject.put("ASSET_ID", sensorId);
					dbObject.put("DATA", data);
					dbObject.put("DATA_DATE", Utils.getDataDateFromFeed(((JSONObject)allFeeds.get(i)).get("created_at")));	
					dbObject.put("CREATE_DATE",Utils.getDateStringInISO(new Date()));
					dbObject.put("MODIFY_DATE",Utils.getDateStringInISO(new Date()));
					dbObject.put("APP_ID",1);
					mongoDb.getCollection("IOT_TAD_ASSET_DATA").save(dbObject);					
				}
			}
    	}
    }
  }