package storm;
import java.util.*;
import org.apache.storm.task.*;
import org.apache.storm.topology.*;
import org.apache.storm.topology.base.*;
import org.apache.storm.tuple.*;
import java.io.*;



public class countword extends BaseBasicBolt  {

	Integer id;
	String name;
	TreeMap<String, Integer> counters;
	List<String> list;
	String fileName;
	static String key;
	static int value;
	static StringBuffer output=new StringBuffer();
	public void prepare(Map stormConf, TopologyContext context) {
		counters = new TreeMap<String, Integer>();
		list=new ArrayList<String>();
		//this.name = context.getThisComponentId();
		//this.id = context.getThisTaskId();
		
		fileName = stormConf.get("dirToWrite").toString();
	}


	
	public void execute(Tuple input,BasicOutputCollector collector) {
		String str = input.getString(0);
		list.add(str);
		if(!counters.containsKey(str)){
			counters.put(str, 1);
		}else{
			
			counters.put(str, counters.get(str)+1);
		}
	}
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}



	


	public void cleanup() {
		
    
		try{
			PrintWriter writer = new PrintWriter(fileName);
			for(Map.Entry<String, Integer> entry : counters.entrySet()){
				
				output.append(entry.getKey()+":"+entry.getValue()+" ");
				if(value<entry.getValue())
				{
					value=entry.getValue();
					key=entry.getKey();
				}	
			}
			writer.println("Trending :"+" "+key);
			writer.println();
			writer.println(output);
			
			writer.close();
			    
		}
		catch (Exception e){}

	}
}