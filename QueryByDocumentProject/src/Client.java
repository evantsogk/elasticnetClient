// Tsogkas Evangelos 3150185

import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import java.io.*;
import java.util.ArrayList;
import java.util.Collections;

/**
 * This class serves as a rest client for elastic search.
 */
public class Client {

    private RestHighLevelClient client;
    private String indexName;

    /**
     *  Constructor. Initializes the rest high level client.
     */
    public Client(String indexName) {
        this.indexName = indexName;
        client = new RestHighLevelClient(
            RestClient.builder(
                new HttpHost("localhost", 9200, "http"),
                new HttpHost("localhost", 9201, "http")));
    }

    /**
     * Closes the rest client.
     */
    public void close() {
        try {
            client.close();
            System.out.println("Client closed");
        }
        catch(IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Checks if the index exists.
     * @return True if the index exists
     */
    public boolean indexExists() {
        try {
            Response response = client.getLowLevelClient().performRequest("HEAD", "/" + indexName);
            int statusCode = response.getStatusLine().getStatusCode();
            return statusCode != 404;
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * Creates the index if it does not exist. Uses low level client.
     *
     * Analysis: standard analyzer (tokenization, lowercase, stopword removal) + stemming filter
     * Similarity: default (BM25)
     */
    public void createIndex() {
        System.out.println("Creating index...");
        try {
            String indexConfiguration = "{\n" +
                    "    \"settings\": {\n" +
                    "        \"analysis\" : {\n" +
                    "            \"analyzer\" : {\n" +
                    "                \"my_analyzer\" : {\n" +
                    "                    \"type\" : \"standard\",\n" +
                    "                    \"stopwords\": \"_english_\",\n" +
                    "                    \"filter\" : {\n" +
                    "                        \"my_stemmer\" : {\n" +
                    "                            \"type\" : \"stemmer\",\n" +
                    "                            \"name\" : \"english\"\n" +
                    "                        }\n" +
                    "                    }\n" +
                    "                }     \n" +
                    "                \n" +
                    "            }\n" +
                    "        },\n" +
                    "        \"index\" : {\n" +
                    "          \"number_of_shards\" : 5, \n" +
                    "          \"similarity\" : {\n" +
                    "              \"default\" : {\n" +
                    "                \"type\" : \"BM25\"\n" +
                    "                }\n" +
                    "            }\n" +
                    "        }\n" +
                    "    },\n" +
                    "   \"mappings\":{\n" +
                    "      \"_doc\": {\n" +
                    "        \"properties\":{\n" +
                    "          \"text\": {\n" +
                    "            \"type\":\"text\",\n" +
                    "            \"analyzer\":\"my_analyzer\",\n" +
                    "            \"term_vector\" : \"yes\"\n" +
                    "          }\n" +
                    "        }\n" +
                    "      }\n" +
                    "   }\n" +
                    "}";
            StringEntity entity = new StringEntity(indexConfiguration, ContentType.APPLICATION_JSON);

            Response response = client.getLowLevelClient().performRequest("PUT", "/" + indexName, Collections.emptyMap(), entity);
            boolean acknowledged = response.getStatusLine().getStatusCode()==200;
            System.out.println("{\n\tacknowledged : " + acknowledged + "\n}");
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Deletes the index.
     */
    public void deleteIndex() {
        DeleteIndexRequest request = new DeleteIndexRequest(indexName);
        System.out.println("Deleting index...");
        try {
            AcknowledgedResponse deleteIndexResponse = client.indices().delete(request);
            boolean response = deleteIndexResponse.isAcknowledged();
            System.out.println("{\n\tacknowledged : " + response + "\n}");
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Inserts the data to the index from the 'texts.json' file located in directory 'output'.
     */
    public void insertData() {
        try {
            System.out.println("Inserting data...");
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new DataInputStream(new FileInputStream("output/texts.json"))));
            JSONParser jsonParser = new JSONParser();
            JSONObject jsonObject;
            String readLine;
            String id="";
            BulkRequest request = new BulkRequest();
            while((readLine = bufferedReader.readLine()) != null) {
                jsonObject = (JSONObject) jsonParser.parse(readLine);
                if (readLine.startsWith("{\"index")) {
                    id = (String) ((JSONObject) jsonObject.get("index")).get("_id");
                }
                else {
                    request.add(new IndexRequest(indexName, "_doc", id)
                            .source(jsonObject));
                }
            }
            BulkResponse bulkResponse = client.bulk(request);
            System.out.println("{\n\ttotal : " + bulkResponse.getItems().length);
            System.out.println("\tfailures : " + bulkResponse.hasFailures());
            System.out.println("\tstatus : " + bulkResponse.status() + "\n}");
        }
        catch (IOException | ParseException e) {
            e.printStackTrace();
        }
    }

    /**
     * Makes a full text query.
     * @param query The query
     * @return A list of pairs(id, score) of documents retrieved
     */
    public ArrayList<float[]> fullTextQuery(String query) {
        ArrayList<float[]> replies = new ArrayList<>();
        int k = 20; //number of documents to retrieve

        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("text", query);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(k+1);
        sourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
        sourceBuilder.query(matchQueryBuilder);

        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(sourceBuilder);
        try {
            SearchResponse searchResponse = client.search(searchRequest);
            SearchHits hits = searchResponse.getHits();
            SearchHit[] searchHits = hits.getHits();

            //i=1 because the 1st hit is the query text itself so we ignore it
            for (int i=1; i<searchHits.length; i++) {
                float[] doc = {Integer.parseInt(searchHits[i].getId()), searchHits[i].getScore()};
                replies.add(doc);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return replies;
    }

    /**
     * Makes an MLT query.
     * @param query The 'like' text.
     * @return A list of pairs(id, score) of documents retrieved
     */
    public ArrayList<float[]> MLTQuery(String query) {
        ArrayList<float[]> replies = new ArrayList<>();
        int k = 20; //number of documents to retrieve
        String[] likeText = {query};
        String[] fields = {"text"};

        MoreLikeThisQueryBuilder mltBuilder = QueryBuilders.moreLikeThisQuery(fields, likeText, null)
                .maxQueryTerms(100)
                .minTermFreq(1)
                .minDocFreq(1)
                .minimumShouldMatch("10%");


        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(k+1);
        sourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
        sourceBuilder.query(mltBuilder);

        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(sourceBuilder);
        try {
            SearchResponse searchResponse = client.search(searchRequest);
            SearchHits hits = searchResponse.getHits();
            SearchHit[] searchHits = hits.getHits();

            //i=1 because the 1st hit is the query text itself so we ignore it
            for (int i=1; i<searchHits.length; i++) {
                float[] doc = {Integer.parseInt(searchHits[i].getId()), searchHits[i].getScore()};
                replies.add(doc);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return replies;
    }
}

