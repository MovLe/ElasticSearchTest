package ES;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
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


}

