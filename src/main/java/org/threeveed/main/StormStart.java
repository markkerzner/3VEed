package org.threeveed.main;

import org.threeveed.bolts.ThreeVEedEmlBolt;
import org.threeveed.spouts.DirectoryReaderSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class StormStart {
    
    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
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
        String custodian = args[4];
        
        Config config = new Config();
        config.put("inputFile", inputDir);
        config.put("solrUrl", solrUrl);
        config.put("caseId", caseId);
        config.put("custodian", custodian);
        config.setDebug(true);
        
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("directory-reader-spout", new DirectoryReaderSpout());
        builder.setBolt("eml-bolt", new ThreeVEedEmlBolt(), numberOfBolts).shuffleGrouping(
                "directory-reader-spout");
        
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("FreeEedStorm", config, builder.createTopology());
    }
    
    private static void printUsageAndExit() {
        System.out.println("Usage: java -jar 3veed.jar <input file/dir> <number of bolts> <solr url> <case id> <custodian>");
        System.exit(-1);
    }
}
