package storm;

import org.apache.storm.*;
import org.apache.storm.topology.*;
import org.apache.storm.tuple.Fields;

public class TopologyMain {

	public static void main(String[] args) throws InterruptedException {
		TopologyBuilder builder=new TopologyBuilder();
		builder.setSpout("File-Reader-Spout", new FileReaderspout());
        builder.setBolt("Simple-bolt", new simpleBase()).shuffleGrouping("File-Reader-Spout");
        builder.setBolt("Count-bolt", new countword(),2).fieldsGrouping("Simple-bolt", new Fields("word"));
        Config conf=new Config();
        conf.setDebug(false);
        conf.put("filereader", "/home/gmsrisailatha/Desktop/example");
        conf.put("dirToWrite","/home/gmsrisailatha/Desktop/output");
        LocalCluster cluster=new LocalCluster();
        
        cluster.submitTopology("Topology-reader", conf, builder.createTopology());
        Thread.sleep(109000);
        cluster.killTopology("Topology-reader");
       // cluster.shutdown();
	}

}
