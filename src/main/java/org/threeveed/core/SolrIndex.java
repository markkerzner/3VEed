/*
 *
 * Copyright SHMsoft, Inc. 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.threeveed.core;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.protocol.HTTP;
import org.apache.tika.metadata.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Create Solr index.
 *
 * Currently implement only creation via HTTP.
 *
 * @author ivanl
 *
 */
public class SolrIndex {

    private static final Logger logger = LoggerFactory.getLogger(SolrIndex.class);
    public static final String SOLR_INSTANCE_DIR = "shmcloud";
    protected boolean supportMultipleProjects = true;
    protected String checkedSolrCloudEndpoint = null;
    protected boolean isInited = false;
    private static AtomicLong solrId = new AtomicLong(0);
    private String updateUrl;
    protected StringBuffer batchBuffer = new StringBuffer(1024 * 1024);
    private String solrUrl;
    private String caseId;
    
    public SolrIndex(String solrUrl, String caseId) {
        this.solrUrl = solrUrl;
        this.caseId = caseId;
        
        init();
    }
    
    protected void sendPostCommand(String point, String param) throws SolrException {
        HttpClient httpClient = new DefaultHttpClient();

        try {
            HttpPost request = new HttpPost(point);
            StringEntity params = new StringEntity(param, HTTP.UTF_8);
            params.setContentType("text/xml");

            request.setEntity(params);

            HttpResponse response = httpClient.execute(request);
            if (response.getStatusLine().getStatusCode() != 200) {
                logger.error("Solr Invalid Response: {}", response.getStatusLine().getStatusCode());
            }

        } catch (Exception ex) {
            logger.error("Problem sending request", ex);
            throw new SolrException("Problem sending request", ex);
        }
    }

    protected void sendGetCommand(String command) throws SolrException {
        HttpClient httpClient = new DefaultHttpClient();

        try {
            HttpGet request = new HttpGet(command);
            HttpResponse response = httpClient.execute(request);
            if (response.getStatusLine().getStatusCode() != 200) {
                logger.error("Solr Invalid Response: {}", response.getStatusLine().getStatusCode());
                throw new SolrException("Invalid response");
            }
        } catch (IOException | SolrException ex) {
            throw new SolrException("Problem sending request", ex);
        }
    }
    
    public void addData(Metadata metadata) {
        if (updateUrl == null) {
            if (isInited) {
                System.err.println("No updateUrl set");
                return;
            }
            resetUpdateUrl();
        }

        try {
            StringBuilder param = new StringBuilder();
            param.append("<add>");
            param.append("<doc>");
            param.append("<field name=\"id\">SOLRID_");
            
            param.append(caseId).append("_");
            param.append(solrId.incrementAndGet());
            param.append("</field>");

            String[] metadataNames = metadata.names();
            for (String name : metadataNames) {
                String data = metadata.get(name);
                param.append("<field name=\"");
                param.append(name);
                param.append("\">");
                param.append("<![CDATA[");
                param.append(filterNotCorrectCharacters(data));
                param.append("]]></field>");
            }

            param.append("</doc></add>");

            sendPostCommand(updateUrl, param.toString());
            sendPostCommand(updateUrl, "<commit/>");
        } catch (SolrException e) {
            logger.error("Error", e);
        }
    }

    private String filterNotCorrectCharacters(String data) {
        return data.replaceAll("[\\x00-\\x09\\x11\\x12\\x14-\\x1F\\x7F]", "");
    }

    public void init() {
        isInited = true;
        String command = null;

        try {
            String endpoint = getSolrEndpoint();

            if (supportMultipleProjects) {
                command = endpoint + "solr/admin/cores?action=CREATE&name=" + SOLR_INSTANCE_DIR + "_" + caseId
                        + "&instanceDir=" + SOLR_INSTANCE_DIR
                        + "&config=solrconfig.xml&dataDir=data_" + caseId
                        + "&schema=schema.xml";
                try {
                    sendGetCommand(command);
                } catch (Exception ex) {
                    logger.error("Unable to create Core: {}", SOLR_INSTANCE_DIR + "_" + caseId);
                    logger.error("Core command: {}", command);
                }

                this.updateUrl = endpoint + "solr/" + SOLR_INSTANCE_DIR + "_" + caseId + "/update";
            } else {
                sendGetCommand(endpoint + "solr/admin/ping");

                this.updateUrl = endpoint + "solr/update";
            }

            String deleteAll = "<delete><query>id:[*TO *]</query></delete>";
            sendPostCommand(updateUrl, deleteAll);
            sendPostCommand(updateUrl, "<commit/>");
        } catch (SolrException se) {
            logger.error("Problem with SOLR init", se);
        }
    }

    protected void resetUpdateUrl() {
        try {
            String endpoint = getSolrEndpoint();

            if (supportMultipleProjects) {
                this.updateUrl = endpoint + "solr/" + SOLR_INSTANCE_DIR + "_" + caseId + "/update";
            } else {
                this.updateUrl = endpoint + "solr/update";
            }
        } catch (SolrException se) {
            logger.error("Problem with SOLR resetUpdateUrl: ", se);
        }
    }

    protected String getSolrEndpoint() throws SolrException {
        String endpoint = solrUrl;

        if (endpoint == null || endpoint.length() == 0) {
            throw new SolrException("Endpoint not configured");
        }

        if (endpoint.endsWith("/")) {
            return endpoint;
        }

        return endpoint + "/";
    }

    private static final class SolrException extends Exception {

        private static final long serialVersionUID = 5904372392164798773L;

        public SolrException(String message) {
            super(message);
        }

        public SolrException(String message, Exception e) {
            super(message, e);
        }
    }
}
