package storm;

import java.util.*;

import org.apache.storm.topology.*;
import org.apache.storm.topology.base.*;
import org.apache.storm.tuple.*;

public class simpleBase extends BaseBasicBolt {
	
	private boolean ignoreWords(String x)
	{
	HashSet<String>list=new HashSet<String>();
	list.add("http");list.add("https");list.add("that");list.add("from");list.add("this");list.add("sample");list.add("test");
	list.add("the");list.add("a");list.add("an");list.add("but");list.add("yet");list.add("was");list.add("are");list.add("were");
	list.add("is");list.add("for");list.add("what");list.add("whose");list.add("how");list.add("why");list.add("which");
	list.add("I");list.add("about");list.add("we");list.add("time");list.add("will");list.add("shall");list.add("would");list.add("should");
	list.add("can");list.add("could");list.add("know");list.add("not");list.add("out");list.add("none");
	return list.contains(x);
	}

	
	public void execute(Tuple input,BasicOutputCollector collector)
	{
		String sentence = input.getString(0);
        String[] words = sentence.split(" ");
        for(String word : words){
            //word = word.trim();
            if(!word.isEmpty() && !ignoreWords(word) ){
                word = word.toLowerCase();
                collector.emit(new Values(word));
            }
        }
		//collector.emit(new Values(input.getString(0)+"After Bolt"));
	}
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields("word"));
	}
	/*public void cleanup()
	{}*/
}
