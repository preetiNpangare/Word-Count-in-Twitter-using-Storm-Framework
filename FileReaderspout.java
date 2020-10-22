package storm;

import java.io.*;
import java.util.*;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.*;
import org.apache.storm.topology.*;
import org.apache.storm.topology.base.*;
import org.apache.storm.tuple.*;

public class FileReaderspout extends BaseRichSpout {

	private FileReader filereader;
	private SpoutOutputCollector collector;
	private String str;
	private BufferedReader reader;
	boolean completed=false;
	
	public void open(Map conf,TopologyContext context,SpoutOutputCollector collector)
	{
		try
		{
		filereader=new FileReader(conf.get("filereader").toString());
		}
		catch(Exception e)
		{
			System.out.println("Error reading file"+e);
		}
		this.collector=collector;
		reader=new BufferedReader(filereader);	
	}
	public void nextTuple()
	{
		while(!completed)
		{
	try {
		str=reader.readLine();
		if(str!=null)
		{
			this.collector.emit(new Values(str));
		}
		else
		{
			completed=true;
			filereader.close();
		}
	   } 
	catch (Exception e) {System.out.println("Error collecting time"+e);}
		}
	}
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields("line"));
	}
	public void ack(Object msgId) {}
	public void fail(Object msgId) {}
}
