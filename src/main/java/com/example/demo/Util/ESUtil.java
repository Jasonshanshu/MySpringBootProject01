package com.example.demo.Util;

import com.baomidou.mybatisplus.core.toolkit.StringUtils;
import com.example.demo.bean.ESProperties;
import io.netty.handler.codec.http.HttpMethod;
import lombok.SneakyThrows;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

/**
 * Created by shanshan on 2020/12/26.
 */
@Component
public class ESUtil {

    @Autowired
    private ESProperties esProperties;

    private RestClient restClient;


    private TransportClient transportClient;

    /**
     * 因为该工具类中所有的方法都是静态方法，静态方法只能调用静态变量
     *  所以使用@Autowired注解所注入进的对象静态方法不能直接调用，因为static修饰的方式不能使用普通变量
     *  下面的@PostConstruct注解就是来解决了以上的问题
     */
    private static TransportClient client;

    private static Map<String, Object> resultMap = new HashMap<String, Object>();

    /**
     * @PostContruct是spring框架的注解 spring容器初始化的时候执行该方法
     */
    @PostConstruct
    public void init() {
        System.out.println("*******************init****************************");
        TransportClient transportClient=null;
        try{
            //2、设置Java对ES的集群信息
            Settings settings=Settings.builder().put("cluster.name",esProperties.getClusterName())
                    .put("node.name", esProperties.getNodeName())
                    .put("client.transport.sniff", true)
                    .put("thread_pool.search.size", esProperties.getPool()).build();
            //3、初始化TransportClient对象
            transportClient=new PreBuiltTransportClient(settings);

            TransportAddress transportAddress=new TransportAddress(InetAddress.getByName(esProperties.getIp()),Integer.parseInt(esProperties.getPort()));
            //5、把对ES的连接对象放到transportClient对象中
            transportClient.addTransportAddress(transportAddress);
        }catch (UnknownHostException e){
            e.printStackTrace();
        }
        System.out.println(transportClient);
        client = transportClient;
    }


    /**
     * 创建索引
     *
     * @param index
     * @return
     */
    public static Map<String, Object> createIndex(String index) {
        System.out.println(client);
        // isIndexExist:判断索引是否存在
        if (!isIndexExist(index)) {
            resultMap.put("code", StatusEnum.EXIST.getCode());
            resultMap.put("msg", StatusEnum.EXIST.getMsg());
        }
        CreateIndexResponse indexresponse = client.admin().indices().prepareCreate(index).execute().actionGet();
        // indexresponse.isAcknowledged():创建索引是否成功，return Boolean类型(true:表示成功，false:失败)
        if(indexresponse.isAcknowledged()) {
            resultMap.put("code", StatusEnum.OPRATION_SUCCESS.getCode());
            resultMap.put("msg", StatusEnum.OPRATION_SUCCESS.getMsg());
        } else {
            resultMap.put("code", StatusEnum.OPRATION_FAILED.getCode());
            resultMap.put("msg", StatusEnum.OPRATION_FAILED.getMsg());
        }
        return resultMap;
    }

    /**
     * 删除索引
     *
     * @param index
     * @return
     */
    public Map<String, Object> deleteIndex(String index) {
        SearchResponse response = client.prepareSearch(index)
                .get();
        SearchHits searchHits = response.getHits();
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            String res = hit.getSourceAsString();
            System.out.println("res" + res);
        }

        if (!isIndexExist(index)) {
            resultMap.put("code", StatusEnum.NOT_EXIST.getCode());
            resultMap.put("msg", StatusEnum.NOT_EXIST.getMsg());
            return resultMap;
        }
        //索引存在，就执行删除
        long s = System.currentTimeMillis();
        DeleteResponse deleteResponse = client.prepareDelete(index,"_doc","10101").get();
        System.out.println("deleteResponse.status()="+deleteResponse.status());
        if(deleteResponse.status().equals("OK")) {
            resultMap.put("code", StatusEnum.OPRATION_SUCCESS.getCode());
            resultMap.put("msg", StatusEnum.OPRATION_SUCCESS.getMsg());
        } else {
            resultMap.put("code", StatusEnum.OPRATION_FAILED.getCode());
            resultMap.put("msg", StatusEnum.OPRATION_FAILED.getMsg());
        }

        return resultMap;
    }


    /**
     * 判断索引是否存在
     *
     * @param index
     * @return
     */
    public static boolean isIndexExist(String index) {
        IndicesExistsResponse inExistsResponse = client.admin().indices().exists(new IndicesExistsRequest(index)).actionGet();
        return inExistsResponse.isExists();
    }

    /**
     * @Author: LX
     * @Description: 判断index下指定type是否存在
     * @Date: 2018/11/6 14:46
     * @Modified by:
     */
    public static boolean isTypeExist(String index, String type) {
        return isIndexExist(index)
                ? client.admin().indices().prepareTypesExists(index).setTypes(type).execute().actionGet().isExists()
                : false;
    }

    /**
     * 数据添加，正定ID
     *
     * @param mapObj 要增加的数据
     * @param index      索引，类似数据库
     * @param type       类型，类似表
     * @param id         数据ID
     * @return
     */
    public static Map<String, Object> addData(Map<String, Object> mapObj, String index, String type, String id) {
        IndexResponse response = client.prepareIndex(index, type, id).setSource(mapObj).get();
        // response.getId():就是添加数据后ES为这条数据所生成的id
        // 需要返回添加数据是否成功
        String status = response.status().toString();


        // 添加数据后所返回的状态(如果成功就是code:200-->OK)
        // eq:sacii --> 小写字母和大写字母不一样
        // status:-->OK
        // ok
        if("OK".equals(status.toUpperCase())||"CREATED".equals(status.toUpperCase())) {
            resultMap.put("code", StatusEnum.OPRATION_SUCCESS.getCode());
            resultMap.put("msg", StatusEnum.OPRATION_SUCCESS.getMsg());
        } else {
            resultMap.put("code", StatusEnum.OPRATION_FAILED.getCode());
            resultMap.put("msg", StatusEnum.OPRATION_FAILED.getMsg());
        }
        return resultMap;
    }

    /**
     * 数据添加
     *
     * @param mapObj 要增加的数据
     * @param index      索引，类似数据库
     * @param type       类型，类似表
     * @return
     */
    public static Map<String, Object> addData(Map<String, Object> mapObj, String index, String type) {
        return addData(mapObj,index, type, UUID.randomUUID().toString().replaceAll("-", "").toUpperCase());
    }

    /**
     * @author 刘其佳
     * @description
     *      将对象转化为map类型
     * @param * param *:object
     * @date 2019/9/19
     * @return java.util.Map<java.lang.String,java.lang.Object>
     * @throws
     */
    public static Map<String, Object> objectTurnMap(Object object){
        Map<String, Object> result = new HashMap<String, Object>();
        //获得类的属性名  数组
        Field[] fields = object.getClass().getDeclaredFields();
        try {
            for (Field field : fields) {
                field.setAccessible(true);
                String name = new String(field.getName());
                result.put(name, field.get(object));
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 通过ID删除数据
     *
     * @param index 索引，类似数据库
     * @param type  类型，类似表
     * @param id    数据ID
     */
    public static Map<String, Object> deleteDataById(String index, String type, String id) {

        DeleteResponse response = client.prepareDelete(index, type, id).execute().actionGet();
        if("OK".equals(response.status().toString().toUpperCase())) {
            resultMap.put("code", StatusEnum.OPRATION_SUCCESS.getCode());
            resultMap.put("msg", StatusEnum.OPRATION_SUCCESS.getMsg());
        } else {
            resultMap.put("code", StatusEnum.OPRATION_FAILED.getCode());
            resultMap.put("msg", StatusEnum.OPRATION_FAILED.getMsg());
        }
        return resultMap;

    }

    /**
     * 通过ID 更新数据
     *
     * @param mapObj 要增加的数据
     * @param index      索引，类似数据库
     * @param type       类型，类似表
     * @param id         数据ID
     * @return
     */
    public static Map<String, Object> updateDataById(Map<String, Object> mapObj, String index, String type, String id) {

        UpdateRequest updateRequest = new UpdateRequest();

        updateRequest.index(index).type(type).id(id).doc(mapObj);

        ActionFuture<UpdateResponse> update = client.update(updateRequest);

        if("OK".equals(update.actionGet().status().toString().toUpperCase())) {
            resultMap.put("code", StatusEnum.OPRATION_SUCCESS.getCode());
            resultMap.put("msg", StatusEnum.OPRATION_SUCCESS.getMsg());
        } else {
            resultMap.put("code", StatusEnum.OPRATION_FAILED.getCode());
            resultMap.put("msg", StatusEnum.OPRATION_FAILED.getMsg());
        }
        return resultMap;
    }

    /**
     * 通过ID获取数据
     *
     * @param index  索引，类似数据库
     * @param type   类型，类似表
     * @param id     数据ID
     * @param fields 需要显示的字段，逗号分隔（缺省为全部字段）
     * @return
     */
    public static Map<String, Object> searchDataById(String index, String type, String id, String fields) {

        GetRequestBuilder getRequestBuilder = client.prepareGet(index, type, id);

        if (StringUtils.isNotEmpty(fields)) {
            getRequestBuilder.setFetchSource(fields.split(","), null);
        }

        GetResponse getResponse = getRequestBuilder.execute().actionGet();

        return getResponse.getSource();
    }

    /**
     * 使用分词查询
     *
     * @param index          索引名称
     * @param type           类型名称,可传入多个type逗号分隔
     * @param query          查询条件
     * @param size           文档大小限制
     * @param fields         需要显示的字段，逗号分隔（缺省为全部字段）
     * @param sortField      排序字段
     * @param highlightField 高亮字段
     * @return
     */
    public static List<Map<String, Object>> searchListData(
            String index, String type, QueryBuilder query, Integer size,
            String fields, String sortField, String highlightField) {

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index);
        if (StringUtils.isNotEmpty(type)) {
            searchRequestBuilder.setTypes(type.split(","));
        }

        if (StringUtils.isNotEmpty(highlightField)) {
            HighlightBuilder highlightBuilder = new HighlightBuilder();
            // 设置高亮字段
            highlightBuilder.field(highlightField);
            searchRequestBuilder.highlighter(highlightBuilder);
        }

        searchRequestBuilder.setQuery(query);

        if (StringUtils.isNotEmpty(fields)) {
            searchRequestBuilder.setFetchSource(fields.split(","), null);
        }
        searchRequestBuilder.setFetchSource(true);

        if (StringUtils.isNotEmpty(sortField)) {
            searchRequestBuilder.addSort(sortField, SortOrder.DESC);
        }

        if (size != null && size > 0) {
            searchRequestBuilder.setSize(size);
        }

        //打印的内容 可以在 Elasticsearch head 和 Kibana  上执行查询

        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

        SearchHit[] totalHits = searchResponse.getHits().getHits();
        long length = searchResponse.getHits().getHits().length;

        if (searchResponse.status().getStatus() == 200) {
            // 解析对象
            return setSearchResponse(searchResponse, highlightField);
        }
        return null;

    }


    /**
     * 高亮结果集 特殊处理
     *
     * @param searchResponse
     * @param highlightField
     */
    private static List<Map<String, Object>> setSearchResponse(SearchResponse searchResponse, String highlightField) {
        List<Map<String, Object>> sourceList = new ArrayList<Map<String, Object>>();
        StringBuffer stringBuffer = new StringBuffer();

        for (SearchHit searchHit : searchResponse.getHits().getHits()) {
            searchHit.getSourceAsMap().put("id", searchHit.getId());

            if (StringUtils.isNotEmpty(highlightField)) {

                System.out.println("遍历 高亮结果集，覆盖 正常结果集" + searchHit.getSourceAsMap());
                Text[] text = searchHit.getHighlightFields().get(highlightField).getFragments();

                if (text != null) {
                    for (Text str : text) {
                        stringBuffer.append(str.string());
                    }
                    //遍历 高亮结果集，覆盖 正常结果集
                    searchHit.getSourceAsMap().put(highlightField, stringBuffer.toString());
                }
            }
            sourceList.add(searchHit.getSourceAsMap());
        }
        return sourceList;
    }

    @SneakyThrows
    public void aggQuery() {
        //待完善
        //需要给聚合内容一个别名
        AggregationBuilder aggregation = AggregationBuilders
                .terms("age").field("field");
        QueryBuilder allQuery = QueryBuilders.matchAllQuery();
        SearchResponse response = client.prepareSearch("javaestest01")
                .setQuery(allQuery).addAggregation(aggregation).get();
        //根据别名获取聚合对象，不同聚合会返回不同的聚合对象
        Terms terms = response.getAggregations().get("age");
        for(Terms.Bucket entry:terms.getBuckets()){
            //聚合的属性值
            String value = entry.getKey().toString();
            //聚合后的数量
            long count = entry.getDocCount();
        }

    }

}
