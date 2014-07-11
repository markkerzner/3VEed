package org.threeveed.main;

import org.threeveed.bolts.WordSpitterBolt;
import org.threeveed.spouts.LineReaderSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class StormStart {
    
    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            printUsageAndExit();
        }
        
        String inputDir = args[0]; 
        int numberOfBolts = 1;
        
        try {
            numberOfBolts = Integer.parseInt(args[1]);
        } catch (Exception e) {
            printUsageAndExit();
        }
        
        String solrUrl = args[2];
        String caseId = args[3];
        
        Config config = new Config();
        config.put("inputFile", inputDir);
        config.put("solrUrl", solrUrl);
        config.put("caseId", caseId);
        config.setDebug(true);
        
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("line-reader-spout", new LineReaderSpout());
        builder.setBolt("word-spitter", new WordSpitterBolt(), numberOfBolts).shuffleGrouping(
                "line-reader-spout");
        
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("FreeEedStorm", config, builder.createTopology());
        
        Thread.sleep(10000);
        cluster.shutdown();
    }
    
    private static void printUsageAndExit() {
        System.out.println("Usage: java -jar 3veed.jar <input file/dir> <number of bolts> <solr url> <case id>");
        System.exit(-1);
    }
}
