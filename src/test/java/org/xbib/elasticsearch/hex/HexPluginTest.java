package org.xbib.elasticsearch.hex;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.search.SearchHit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xbib.elasticsearch.action.bulk.BulkAction;
import org.xbib.elasticsearch.action.bulk.BulkRequest;
import org.xbib.elasticsearch.common.xcontent.XContentBuilder;
import org.xbib.elasticsearch.common.xcontent.XContentFactory;
import org.xbib.elasticsearch.common.xcontent.XContentParser;
import org.xbib.elasticsearch.common.xcontent.XContentType;
import org.xbib.elasticsearch.helper.AbstractNodesTests;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;

import static org.junit.Assert.assertEquals;
import static org.xbib.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class HexPluginTest extends AbstractNodesTests {

    private final static ESLogger logger = ESLoggerFactory.getLogger("Test");

    private Client client;

    @Before
    public void createNodes() throws Exception {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("index.number_of_shards", numberOfShards())
                .put("index.number_of_replicas", 0)
                .build();
        for (int i = 0; i < numberOfNodes(); i++) {
            startNode("node" + i, settings);
        }
        client = getClient();
    }

    protected int numberOfShards() {
        return 1;
    }

    protected int numberOfNodes() {
        return 1;
    }

    @After
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("node0");
    }

    @Test
    public void testBulkAction() throws Exception {
        Client client = getClient();
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < 10; i++) {
            String source = "{\"hex\":\"4AC3B67267\", \"nothex\":\"Hello World\"}";
            XContentParser contentParser = XContentFactory
                    .xContent(XContentType.JSON)
                    .createParser(source)
                    .enableBase16Checks(true);
            XContentBuilder builder = jsonBuilder().copyCurrentStructure(contentParser);
            bulkRequest.add(new IndexRequest("test", "test", Integer.toString(i))
                    .source(builder.string()));
        }
        client.execute(BulkAction.INSTANCE, bulkRequest).actionGet();
        client.admin().indices().refresh(new RefreshRequest("test")).actionGet();
        SearchRequest searchRequest = new SearchRequest();
        SearchResponse searchResponse = client.search(searchRequest).actionGet();
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            logger.info("api search hit = {}", hit.getSourceAsString());
            // here we see the base64 encoding of the base16 binary
            assertEquals("{\"hex\":\"SsO2cmc=\",\"nothex\":\"Hello World\"}", hit.getSourceAsString());
        }
        client.admin().indices().delete(new DeleteIndexRequest("test"));
    }

    @Test
    public void testHttp() throws Exception {
        URL url = new URL(getHttpAddressOfNode("node0").toURL(), "/_bulkhex");
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("POST");
        connection.setDoOutput(true);
        String bulk =
                "{\"index\":{\"_index\":\"test\",\"_type\":\"test\",\"_id\":\"1\"}\n" +
                "{\"hex\":\"4AC3B67267\", \"nothex\":\"Hello HTTP World\"}\n" +
                "{\"index\":{\"_index\":\"test\",\"_type\":\"test\",\"_id\":\"2\"}\n" +
                "{\"hex\":\"4AC3B67267\", \"nothex\":\"Hello HTTP World\"}\n" +
                "{\"index\":{\"_index\":\"test\",\"_type\":\"test\",\"_id\":\"3\"}\n" +
                "{\"hex\":\"4AC3B67267\", \"nothex\":\"Hello HTTP World\"}\n" +
                "{\"index\":{\"_index\":\"test\",\"_type\":\"test\",\"_id\":\"4\"}\n" +
                "{\"hex\":\"4AC3B67267\", \"nothex\":\"Hello HTTP World\"}\n" +
                "{\"index\":{\"_index\":\"test\",\"_type\":\"test\",\"_id\":\"5\"}\n" +
                "{\"hex\":\"4AC3B67267\", \"nothex\":\"Hello HTTP World\"}\n" +
                "{\"index\":{\"_index\":\"test\",\"_type\":\"test\",\"_id\":\"6\"}\n" +
                "{\"hex\":\"4AC3B67267\", \"nothex\":\"Hello HTTP World\"}\n" +
                "{\"index\":{\"_index\":\"test\",\"_type\":\"test\",\"_id\":\"7\"}\n" +
                "{\"hex\":\"4AC3B67267\", \"nothex\":\"Hello HTTP World\"}\n" +
                "{\"index\":{\"_index\":\"test\",\"_type\":\"test\",\"_id\":\"8\"}\n" +
                "{\"hex\":\"4AC3B67267\", \"nothex\":\"Hello HTTP World\"}\n" +
                "{\"index\":{\"_index\":\"test\",\"_type\":\"test\",\"_id\":\"9\"}\n" +
                "{\"hex\":\"4AC3B67267\", \"nothex\":\"Hello HTTP World\"}\n" +
                "{\"index\":{\"_index\":\"test\",\"_type\":\"test\",\"_id\":\"10\"}\n" +
                "{\"hex\":\"4AC3B67267\", \"nothex\":\"Hello HTTP World\"}\n"
                ;
        OutputStreamWriter out = new OutputStreamWriter(connection.getOutputStream());
        out.write(bulk);
        out.close();
        BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        String line;
        while ((line = reader.readLine()) != null) {
            logger.info("http bulk response = {}", line);
        }
        reader.close();
        // refresh
        url = new URL(getHttpAddressOfNode("node0").toURL(), "/test/_refresh");
        connection = (HttpURLConnection) url.openConnection();
        connection.setDoOutput(true);
        connection.setRequestMethod("POST");
        out = new OutputStreamWriter(connection.getOutputStream());
        out.write("\n");
        out.close();
        reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        while ((line = reader.readLine()) != null) {
            logger.info("http refresh response = {}", line);
        }
        reader.close();
        // search
        url = new URL(getHttpAddressOfNode("node0").toURL(), "/test/test/_search?pretty");
        connection = (HttpURLConnection) url.openConnection();
        connection.setDoOutput(true);
        connection.setRequestMethod("POST");
        out = new OutputStreamWriter(connection.getOutputStream());
        out.write("{\"query\":{\"match_all\":{}}}\n");
        out.close();
        reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        while ((line = reader.readLine()) != null) {
            // in the search reponse, we see base64 encodings. Just log them.
            logger.info("http search response = {}", line);
        }
        reader.close();
        // delete index
        url = new URL(getHttpAddressOfNode("node0").toURL(), "/test");
        connection = (HttpURLConnection) url.openConnection();
        connection.setDoOutput(true);
        connection.setRequestMethod("DELETE");
        out = new OutputStreamWriter(connection.getOutputStream());
        out.write("\n");
        out.close();
    }

}
