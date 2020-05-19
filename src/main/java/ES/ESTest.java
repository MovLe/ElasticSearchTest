package ES;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName ESTest
 * @MethodDesc: 使用API操作ES
 * @Author Movle
 * @Date 5/19/20 12:36 下午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/


public class ESTest {

    // 对ES的操作都是通过client
    private TransportClient client;

    @SuppressWarnings("unchecked")
    @Before
    public void getClient() throws UnknownHostException {
        // 1 设置连接集群的名称
        Settings settings = Settings.builder().put("cluster.name","my-application").build();

        // 2 连接集群
        client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.31.132"),9300));
    }

    //创建索引
    @Test
    public void createIndex_blog(){
        client.admin().indices().prepareCreate("blog").get();
        client.close();
    }

    //删除索引
    @Test
    public void deleteIndex() {
        client.admin().indices().prepareDelete("blog").get();

        client.close();
    }
    @Test
    public void createDocByJson() {
        // 使用json 创建document

        String json = "{" + "\"id\":\"1\"," + "\"title\":\"基于Lucene的搜索服务器\","
                + "\"content\":\"它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口\"" + "}";

        // 创建文档
        IndexResponse indexResponse = client.prepareIndex("blog", "article", "1").setSource(json).execute().actionGet();

        // 打印返回结果
        System.out.println("index: " + indexResponse.getIndex());
        System.out.println("type: " + indexResponse.getType());
        System.out.println("id: " + indexResponse.getId());
        System.out.println("version: " + indexResponse.getVersion());
        System.out.println("result: " + indexResponse.getResult());

        client.close();

    }

    @Test
    public void createDocByMap() {

        Map<String, Object> json = new HashMap<String, Object>();
        json.put("id", "2");
        json.put("title", "基于Lucene的搜索服务器");
        json.put("content", "它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口");

        // 创建文档
        IndexResponse indexResponse = client.prepareIndex("blog2", "article").setSource(json).execute().actionGet();

        // 打印返回结果
        System.out.println("index: " + indexResponse.getIndex());
        System.out.println("type: " + indexResponse.getType());
        System.out.println("id: " + indexResponse.getId());
        System.out.println("version: " + indexResponse.getVersion());
        System.out.println("result: " + indexResponse.getResult());

        client.close();

    }

    @Test
    public void createDocByXContent() throws Exception {
        // 使用xcontent创建document

        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().field("id", "3")
                .field("title", "基于Lucene的搜索服务器").field("content", "它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口").endObject();

        // 创建文档
        IndexResponse indexResponse = client.prepareIndex("blog3", "article").setSource(builder).execute().actionGet();

        // 打印返回结果
        System.out.println("index: " + indexResponse.getIndex());
        System.out.println("type: " + indexResponse.getType());
        System.out.println("id: " + indexResponse.getId());
        System.out.println("version: " + indexResponse.getVersion());
        System.out.println("result: " + indexResponse.getResult());

        client.close();

    }
    @Test
    public void getData() {

        // 查询文档
        GetResponse response = client.prepareGet("blog", "article", "1").get();

        System.out.println(response.getSourceAsString());

        client.close();
    }

    /**
     * 查询数据
     */
    @Test
    public void getMultiData() {

        MultiGetResponse response = client.prepareMultiGet().add("blog", "article", "1")
                .add("blog", "article", "6", "KCuVCnAnTWeb5F8pNexJIg").add("blog", "article", "KCuVCnAnTWeb5F8pNexJIg")
                .get();

        for (MultiGetItemResponse multiGetItemResponse : response) {
            GetResponse getResponse = multiGetItemResponse.getResponse();

            if (getResponse.isExists()) {
                String sourceAsString = getResponse.getSourceAsString();
                System.out.println(sourceAsString);
            }

        }

        client.close();

    }

    /**
     * 更新数据
     * @throws Exception
     */
    @Test
    public void updateData() throws Exception {

        // 1 创建更新数据的请求对象
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index("blog");
        updateRequest.type("article");
        updateRequest.id("1");

        updateRequest.doc(XContentFactory.jsonBuilder().startObject()
                // 对没有的字段添加, 对已有的字段替换
                .field("title", "基于Lucene的搜索服务器").field("content", "它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口。大数据前景无限")
                .field("createDate", "2017-8-22").endObject());

        UpdateResponse indexResponse = client.update(updateRequest).get();

        // 打印返回结果
        System.out.println("index: " + indexResponse.getIndex());
        System.out.println("type: " + indexResponse.getType());
        System.out.println("id: " + indexResponse.getId());
        System.out.println("version: " + indexResponse.getVersion());
        System.out.println("result: " + indexResponse.getResult());

        client.close();

    }

    /**
     *  更新或者插入，如果没有查到该数据，则插入此数据，若有该数据，则更新
     */
    @Test
    public void testUpdateOrInsert() throws Exception {

        IndexRequest indexRequest = new IndexRequest("blog", "article", "6")
                .source(XContentFactory.jsonBuilder().startObject().field("title", "搜索服务器")
                        .field("content",
                                "它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口。Elasticsearch是用Java开发的，并作为Apache许可条款下的开放源码发布，是当前流行的企业级搜索引擎。设计用于云计算中，能够达到实时搜索，稳定，可靠，快速，安装使用方便。")
                        .endObject());

        UpdateRequest upsert = new UpdateRequest("blog", "article", "6")
                .doc(XContentFactory.jsonBuilder().startObject().field("user", "李四").endObject()).upsert(indexRequest);

        client.update(upsert).get();

        client.close();

    }

    /**
     * 删除数据
     */
    @Test
    public void deleteData() {

        DeleteResponse indexResponse = client.prepareDelete("blog", "article", "6").get();

        // 打印返回结果
        System.out.println("index: " + indexResponse.getIndex());
        System.out.println("type: " + indexResponse.getType());
        System.out.println("id: " + indexResponse.getId());
        System.out.println("version: " + indexResponse.getVersion());
        System.out.println("result: " + indexResponse.getResult());

        client.close();
    }

    /**
     * 类似查询
     */
    @Test
    public void query() {

        // 类似于like 对所有字段like
        SearchResponse searchResponse = client.prepareSearch("blog").setTypes("article")
                .setQuery(QueryBuilders.queryStringQuery("基于")).get();

        SearchHits hits = searchResponse.getHits();
        System.out.println("查询结果有： " + hits.getTotalHits() + " 条");

        for (SearchHit searchHit : hits) {
            System.out.println(searchHit.getSourceAsString());
        }

        client.close();
    }

    /**
     * 通配符查询
     */
    @Test
    public void wildcardQuery() {

        // 通配符查询 *全* 类似于 %全%

        SearchResponse searchResponse = client.prepareSearch("blog").setTypes("article")
                .setQuery(QueryBuilders.wildcardQuery("content", "*前景*")).get();

        SearchHits hits = searchResponse.getHits();
        System.out.println("查询结果有： " + hits.getTotalHits() + " 条");

        for (SearchHit searchHit : hits) {
            System.out.println(searchHit.getSourceAsString());
        }

        client.close();
    }

    @Test
    public void termQuery() {
        // 类似于 mysql 中的 =
        // 不是与字段 = 是与字段的分词结果 =

        SearchResponse searchResponse = client.prepareSearch("blog").setTypes("article")
                .setQuery(QueryBuilders.termQuery("content", "web")).get();

        SearchHits hits = searchResponse.getHits();
        System.out.println("查询结果有： " + hits.getTotalHits() + " 条");

        for (SearchHit searchHit : hits) {
            System.out.println(searchHit.getSourceAsString());
        }

        client.close();
    }

    /**
     * 模糊查询
     */
    @Test
    public void fuzzy() {

        // 模糊查询
        SearchResponse searchResponse = client.prepareSearch("blog").setTypes("article")
                .setQuery(QueryBuilders.fuzzyQuery("title", "服务器")).get();

        SearchHits hits = searchResponse.getHits();
        System.out.println("查询结果有： " + hits.getTotalHits() + " 条");

        for (SearchHit searchHit : hits) {
            System.out.println(searchHit.getSourceAsString());
        }

        client.close();
    }

    @Test
    public void createMapping() throws Exception {

        // 设置mapping
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("article")
                .startObject("properties").startObject("id").field("type", "text").field("store", "true").endObject()
                .startObject("title").field("type", "text").field("store", "false").endObject().startObject("content")
                .field("type", "text").field("store", "true").endObject().endObject().endObject().endObject();

        // 添加mapping
        PutMappingRequest mappingRequest = Requests.putMappingRequest("blog11182").type("article").source(builder);

        client.admin().indices().putMapping(mappingRequest).get();
        // 关闭资源
        client.close();
    }

    @Test
    public void createMapping_ik() throws Exception {
        // 1设置mapping
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("article")
                .startObject("properties").startObject("id").field("type", "text").field("store", "true")
                .field("analyzer", "ik_smart").endObject().startObject("title").field("type", "text")
                .field("store", "false").field("analyzer", "ik_smart").endObject().startObject("content")
                .field("type", "text").field("store", "true").field("analyzer", "ik_smart").endObject().endObject()
                .endObject().endObject();

        // 2 添加mapping
        PutMappingRequest mapping = Requests.putMappingRequest("blog11182").type("article").source(builder);
        client.admin().indices().putMapping(mapping).get();

        // 3 关闭资源
        client.close();

    }

    // 创建文档,以map形式
    @Test
    public void createDocumentByMap_forik() {

        HashMap<String, String> map = new HashMap<String, String>();
        map.put("id", "2");
        map.put("title", "Lucene");
        map.put("content", "它提供了一个分布式的web接口");

        IndexResponse response = client.prepareIndex("blog11182", "article", "3").setSource(map).execute().actionGet();

        // 打印返回的结果
        System.out.println("结果:" + response.getResult());
        System.out.println("id:" + response.getId());
        System.out.println("index:" + response.getIndex());
        System.out.println("type:" + response.getType());
        System.out.println("版本:" + response.getVersion());

        // 关闭资源
        client.close();
    }

    // 词条查询
    @Test
    public void queryTerm_forik() {

        // 分析结果：因为是默认被standard
        // analyzer分词器分词，大写字母全部转为了小写字母，并存入了倒排索引以供搜索。term是确切查询， 必须要匹配到大写的Name。
        SearchResponse response = client.prepareSearch("blog11182").setTypes("article")
                .setQuery(QueryBuilders.termQuery("content", "分布式")).get();

        // 获取查询命中结果
        SearchHits hits = response.getHits();

        System.out.println("结果条数:" + hits.getTotalHits());

        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }


}

