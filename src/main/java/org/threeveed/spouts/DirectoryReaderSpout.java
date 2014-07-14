package org.threeveed.spouts;

import java.io.File;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class DirectoryReaderSpout implements IRichSpout {
    private static final long serialVersionUID = 1L;
    
    private SpoutOutputCollector collector;
    private boolean completed = false;
    private TopologyContext context;
    private String inputDir;

    @Override
    public void open(Map conf, TopologyContext context,
            SpoutOutputCollector collector) {
        
        this.context = context;
        this.inputDir = conf.get("inputFile").toString();
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        if (completed) {
            return;
        }
        
        try {
            File f = new File(inputDir);
            emitFile(f);
        } catch (Exception e) {
            throw new RuntimeException("Error reading tuple", e);
        } finally {
            completed = true;
        }

    }

    private void emitFile(File file) {
        if (file.isDirectory()) {
            String[] filesInDir = file.list();
            for (String fileInDir : filesInDir) {
                String name = file.getAbsolutePath() + File.separator + fileInDir;
                File newFile = new File(name);
                emitFile(newFile);
            }
        } else {
            String fileName = file.getAbsolutePath();
            String ext = FilenameUtils.getExtension(fileName);
            if ("eml".equalsIgnoreCase(ext)) {
                this.collector.emit(new Values(fileName));
            }
        }
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("file"));
    }

    @Override
    public void close() {
    }

    public boolean isDistributed() {
        return false;
    }

    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {
    }

    @Override
    public void ack(Object msgId) {
    }

    @Override
    public void fail(Object msgId) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
