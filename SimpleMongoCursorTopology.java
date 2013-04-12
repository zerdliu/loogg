package com.baidu.mongo.test;

import static backtype.storm.utils.Utils.tuple;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.List;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoURI;

public class SimpleMongoCursorTopology {

	/**
	 * @param args
	 * @throws UnknownHostException 
	 */
	public static void main(String[] args) throws UnknownHostException {
		// TODO Auto-generated method stub
		final String mongoHost = "10.65.43.129";
		final int mongoPort = 8270;
		final String mongoDbName = "test";
		final String mongoCollectionName = "as";
		Mongo mongo = new Mongo(new MongoURI("mongodb://" + mongoHost + ":" + mongoPort));
		//mongo.dropDatabase("mongostorm");
		final BasicDBObject options = new BasicDBObject(); 
		options.put("size", 10000);
		mongo.getDB(mongoDbName).createCollection(mongoCollectionName, options);
		final DBCollection coll = mongo.getDB(mongoDbName).getCollection(mongoCollectionName);
		TopologyBuilder builder = new TopologyBuilder();
		
		/*
		 * wired java function
		 * Anonymous Inner Class initialization
		 * must override unimplemented functions
		 */
		MongoSpout spout = new MongoSpout(mongoHost, mongoPort, mongoDbName, mongoCollectionName, new BasicDBObject()) {

			private static final long serialVersionID = 8888L;
			
			@Override
			public void declareOutputFields(OutputFieldsDeclarer declarer) {
				// TODO Auto-generated method stub
				declarer.declare(new Fields("mongoStorm"));
			}

			@Override
			public List<Object> dbObjectToStormTuple(DBObject message) {
				// TODO Auto-generated method stub
				return tuple(message);
			}
			
		};	
		
		builder.setSpout("1", spout);
		
		Config conf = new Config();
		conf.setDebug(true);
		
		LocalCluster cluster = new LocalCluster();
		
		cluster.submitTopology("mongoStorm", conf, builder.createTopology());
		
		Runnable writer = new Runnable() {

			@Override
			public void run() {
				// TODO Auto-generated method stub
				for(int i = 0; i < 1000; i++) {
					final BasicDBObject doc = new BasicDBObject("_id", i);
					doc.put("ts", new Date());
					coll.insert(doc);
				}
			}
			
		}; 
		
		new Thread(writer).start();
		
		Utils.sleep(10000);
		cluster.shutdown();
		
	}

}
