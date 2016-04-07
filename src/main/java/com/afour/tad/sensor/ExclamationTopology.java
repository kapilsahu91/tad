package com.afour.tad.sensor;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import java.util.Map;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.spout.SchemeAsMultiScheme;
import java.sql.*;
import javax.ws.rs.core.MediaType;
public class ExclamationTopology {

static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
static final String DB_URL = "jdbc:mysql://localhost/TAD";
static final String USER = "root";
static final String PASS = "root";
static Connection conn = null;
static PreparedStatement stmt = null;
	
  public static class ExclamationBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	OutputCollector _collector;
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
    }
    @Override
    public void execute(Tuple tuple) {
		try {
			String sentence = tuple.getString(0);
			JSONParser jsonParser = new JSONParser();
			JSONObject jsonObject = (JSONObject) jsonParser.parse(sentence);
			
			/*String sensor= (String) jsonObject.get("sensor").toString();
			JSONObject sensor_obj = (JSONObject) jsonParser.parse(sensor);
			System.out.println("the json object:----------------------->" + jsonObject);
			String device= (String) jsonObject.get("device").toString();
			JSONObject device_obj = (JSONObject) jsonParser.parse(device);*/
			//ExclamationTopology.insertDataMySql(device_id, device_data);
			ExclamationTopology.insertDataMySqlNew(jsonObject);
			//ExclamationTopology.insertDataMongo(sensor_id, sensor_data);
			//_collector.emit(new Values(sensor_id,sensor_data));
		}
		catch(ParseException ex) {
		ex.printStackTrace();
		}
		catch(Exception ex) {
		ex.printStackTrace();
		}
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("sensor_id","sensor_data"));
    }
  }

/*  private static void insertDataMySql(JSONObject sensor_obj,JSONObject device_obj){
	  String sensor_id = (String) sensor_obj.get("sensor_id");
	  System.out.println("The Sensor Id is: " + sensor_id);
	  String sensor_data= String.valueOf(sensor_obj.get("sensor_data"));
	  System.out.println("The WaterLevel Distance : " + sensor_data);
	  String device_id= String.valueOf(device_obj.get("device_id"));
	  System.out.println("The WaterLevel Distance : " + device_id);
	  
	  try{
	  	URL url = new URL("http://localhost:8080/TestWebApp/rest/mongo/insert");
	  	Client client = Client.create();
	  	
	  	String input = "{\"sensor_id\":\""+sensor_id+"\",\"sensor_data\":\""+sensor_data+"\", \"device_id\":\""+device_id+"\"}";
	  	System.out.println("trying to post json :--------> " + input);
		WebResource webResource = client
		   .resource("http://localhost:8080/TestWebApp/rest/mongo/insert");
		System.out.println("----------------------------> sendig the post request to localhost");
		
		ClientResponse response = webResource.type(MediaType.APPLICATION_JSON)
                   .post(ClientResponse.class,input);

		if (response.getStatus() != 200) {
		   throw new RuntimeException("Failed : HTTP error code : "
			+ response.getStatus());
		}

		//String output = response.getEntity(String.class);
	  }catch(IOException ex){
		  ex.printStackTrace();
	  }
  }*/
  private static void insertDataMySqlNew(JSONObject jsonObject1){
	  
	  
	  //-- new configs appended to take up channel details starts
	  JSONObject channnelJson= (JSONObject)jsonObject1.get("channel");
	  
	  Client client1 = Client.create();
	  WebResource webResource1 = client1
			  .resource("http://localhost:8080/TestWebApp/rest/mongo/insertchannel");
	  System.out.println("--> sendig the post request to localhost for channel"+channnelJson);
	  
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
	  
		 // System.out.println("----------sadfasdfasdfasfdsafasdfdasf---------->"+jsonObject);
		  
		  //long channel_id = (long) jsonObject.get("channel_id");
		  /*String created_at = String.valueOf(jsonObject.get("created_at"));
		  
		  int field1 = 0,field2 = 0,field3 = 0,field4 = 0,field5 = 0;
		  
		  if(jsonObject.get("field1")!=null)
			  field1= Integer.parseInt(String.valueOf(jsonObject.get("field1")).trim());
		  if(jsonObject.get("field2")!=null)
			  field2 = Integer.parseInt(String.valueOf(jsonObject.get("field2")).trim());
		  if(jsonObject.get("field3")!=null)
			  field3= Integer.parseInt(String.valueOf(jsonObject.get("field3")).trim());
		  if(jsonObject.get("field4")!=null)
			  field4= Integer.parseInt(String.valueOf(jsonObject.get("field4")).trim());
		  if(jsonObject.get("field5")!=null)
			  field5 = Integer.parseInt(String.valueOf(jsonObject.get("field5")).trim());*/
		  
		  try{
		  	Client client = Client.create();
		  	/*
		  	String input = "{\"channel_id\":\""+channel_id+"\",\"timestamp\":\""+created_at+"\""
		  			+ ", \"field1\":\""+field1+"\", \"field2\":\""+field2+"\","
		  			+ " \"field3\":\""+field3+"\", \"field4\":\""+field4+"\","
		  			+ " \"field5\":\""+field5+"\"}";*/
		  	//System.out.println("trying to post json :--------> " + jsonObject);
			WebResource webResource = client
			   .resource("http://localhost:8080/TestWebApp/rest/mongo/insertnew");
			System.out.println("--> sendig the post request to localhost for sensor's data");
			
			ClientResponse response = webResource.type(MediaType.APPLICATION_JSON)
					.post(ClientResponse.class,jsonObject.toJSONString());
	
			if (response.getStatus() != 200) {
			   throw new RuntimeException("Failed : HTTP error code : "
				+ response.getStatus());
			}
	
			//String output = response.getEntity(String.class);
		  }catch(Exception ex){
			  ex.printStackTrace();
		  }
      }
  }
  /*private static void insertDataMySqlDevice(String device_id, String device_data , String timestamp){
	  try{
		conn = MySQLConnection.getConnection();
		stmt = conn.prepareStatement("INSERT INTO device (device_id, device_data , timestamp) VALUES (1, 'A', 19) ON DUPLICATE KEY UPDATE id = id + 1;");
		stmt.setString(1,sensor_id);
		stmt.setString(2,sensor_data);
		stmt.execute();
		//conn.close();
	  }catch(SQLException  ex){
		  ex.printStackTrace();
	  }
  }*/
  
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
