package org.threeveed.main;

import org.threeveed.bolts.WordCounterBolt;
import org.threeveed.bolts.WordSpitterBolt;
import org.threeveed.spouts.LineReaderSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class StormStart {
    
    public static void main(String[] args) throws Exception {
        Config config = new Config();
        config.put("inputFile", "c:\\work\\mark\\copy_solr_core_instruction.txt");
        config.setDebug(true);
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("line-reader-spout", new LineReaderSpout());
        builder.setBolt("word-spitter", new WordSpitterBolt()).shuffleGrouping(
                "line-reader-spout");
        builder.setBolt("word-counter", new WordCounterBolt()).shuffleGrouping(
                "word-spitter");
        
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("HelloStorm", config, builder.createTopology());
        Thread.sleep(10000);
        cluster.shutdown();
    }
}
