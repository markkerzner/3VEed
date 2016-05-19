package org.threeveed.bolts;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.tika.Tika;
import org.apache.tika.io.TikaInputStream;
import org.threeveed.core.DocumentMetadata;
import org.threeveed.core.EmlParser;
import org.threeveed.core.SolrIndex;


import org.apache.storm.topology.IRichBolt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Fields;

public class ThreeVEedEmlBolt implements IRichBolt {
    private static final long serialVersionUID = 1L;
    
    private OutputCollector collector;
    private Tika tika;
    private String inputDir;
    private SolrIndex solrIndex;
    private String custodian;
    
    @Override
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        this.collector = collector;
        
        tika = new Tika();
        tika.setMaxStringLength(10 * 1024 * 1024);
        inputDir = stormConf.get("inputFile").toString();
        String solrUrl = stormConf.get("solrUrl").toString();
        String caseId = stormConf.get("caseId").toString();
        custodian = stormConf.get("custodian").toString();
        
        solrIndex = new SolrIndex(solrUrl, caseId);
    }

    @Override
    public void execute(Tuple input) {
        String fileName = input.getString(0);
        File file = new File(fileName);
        
        DocumentMetadata metadata = new DocumentMetadata();
        TikaInputStream inputStream = null;
        
        try {
            metadata.setOriginalPath(getOriginalDocumentPath(fileName));
            
            EmlParser emlParser = new EmlParser(file);
            extractEmlFields(fileName, metadata, emlParser);
            
            inputStream = TikaInputStream.get(file);
            String text = tika.parseToString(inputStream, metadata);
            metadata.set("text", text);

            parseDateTimeReceivedFields(metadata);
            parseDateTimeSentFields(metadata, emlParser.getSentDate());
            
            metadata.setCustodian(custodian);
            
            solrIndex.addData(metadata);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (Exception e) {
                    e.printStackTrace(System.out);
                }
            }
        }
        
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    @Override
    public void cleanup() {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
    
    private void extractEmlFields(String fileName, DocumentMetadata metadata, EmlParser emlParser) {
        try {
            String text = prepareContent(emlParser.getContent());
            List<String> attachments = emlParser.getAttachmentNames();
            if (attachments.size() > 0) {
                text += "<br/>=====================================<br/>Attachments:<br/><br/>";

                for (String att : attachments) {
                    if (att != null) {
                        text += att + "<br/>";
                    }
                }
            }

            metadata.set("text", text);
            if (emlParser.getFrom() != null) {
                metadata.setMessageFrom(getAddressLine(emlParser.getFrom()));
            }

            if (emlParser.getSubject() != null) {
                metadata.setMessageSubject(emlParser.getSubject());
            }

            if (emlParser.getTo() != null) {
                metadata.setMessageTo(getAddressLine(emlParser.getTo()));
            }

            if (emlParser.getCC() != null) {
                metadata.setMessageCC(getAddressLine(emlParser.getCC()));
            }

            if (emlParser.getDate() != null) {
                metadata.setMessageCreationDate(formatDate(emlParser.getDate()));
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    private static String prepareContent(String content) {
        StringBuilder result = new StringBuilder();

        String[] lines = content.split("\n");
        for (String line : lines) {
            result.append(line.replaceAll("<", "&lt;").replaceAll(">", "&gt;"));
            result.append("<br/>");
        }

        return result.toString();
    }

    private static String formatDate(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return sdf.format(date);
    }

    private static String getAddressLine(List<String> addresses) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < addresses.size(); i++) {
            String address = addresses.get(i);
            result.append(address);
            if (i < addresses.size() - 1) {
                result.append(" , ");
            }
        }
        return result.toString();
    }
    
    private void parseDateTimeSentFields(DocumentMetadata metadata, Date sentDate) {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        String date = df.format(sentDate);
        parseDateTimeFields(metadata, date);
    }

    private void parseDateTimeReceivedFields(DocumentMetadata metadata) {
        String date = metadata.getMessageDate();
        parseDateTimeFields(metadata, date);
    }
    
    private void parseDateTimeFields(DocumentMetadata metadata, String date) {
        if (date != null && date.length() > 0) {
            try {
                SimpleDateFormat df = null;
                if (date.startsWith("00")) {
                    df = new SimpleDateFormat("'00'yy-MM-dd'T'HH:mm:ss'Z'");
                } else {
                    df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                }

                Date dateObj = df.parse(date);

                SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyyMMdd");
                dateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
                String dateOnly = dateFormatter.format(dateObj);

                metadata.setMessageDate(dateOnly);
                metadata.setMessageDateReceived(dateOnly);

                SimpleDateFormat timeFormatter = new SimpleDateFormat("HH:mm");
                timeFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
                String timeOnly = timeFormatter.format(dateObj);

                metadata.setMessageTimeReceived(timeOnly);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
    private String getOriginalDocumentPath(String fileName) {
        return fileName != null ? fileName.replace(inputDir, "") : "";
    }
}
