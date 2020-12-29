package com.example.demo.serviceImpl;

import com.example.demo.Util.ESUtil;
import com.example.demo.bean.User;
import com.example.demo.mapper.UserMapper;
import com.example.demo.service.UserService;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by shanshan on 2020/12/24.
 */
@Service
public class UserServiceImpl extends UserService {

    @Resource
    private UserMapper userMapper;

    @Resource
    private ESUtil esUtil;



    @Override
    public User loginIn(String name, String password) {
        return userMapper.getInfo(name,password);
    }

    public static Map<String,Object> resultMap = new HashMap<String,Object>();

    /**
     * @author 刘其佳
     * @description
     *      创建index
     * @param * param *:index
     * @date 2019/9/18
     * @return java.util.Map<java.lang.String,java.lang.Object>
     * @throws
     */
    public Map<String,Object> createIndex(String index){
        return ESUtil.createIndex(index);
    }

    /**
     * @author 刘其佳
     * @description
     *      删除index
     * @param * param *:index
     * @date 2019/9/18
     * @return java.util.Map<java.lang.String,java.lang.Object>
     * @throws
     */
    public Map<String, Object> deleteIndex(String index){
       return esUtil.deleteIndex(index);
    }

    /**
     * @author 刘其佳
     * @description
     *      向ES的索引库添加一条数据
     *          java.lang.IllegalArgumentException: The number of object passed must be even but was [1]:
     *          参数不合法异常
     *          在ES的6.x版本或者以上，废弃掉了JSON对象传递数据，只能用Map传递
     * @param * param *:
     * @date 2019/9/18
     * @return java.util.Map<java.lang.String,java.lang.Object>
     * @throws
     */
    public Map<String,Object> addData(){
        // ES中不能再使用实体类，只能通过JSONObject对象进行传递数据来代替实体类
       /* JSONObject jsonObject = new JSONObject();
        jsonObject.put("id", 20L);
        jsonObject.put("username", "lisi");
        jsonObject.put("password", "123456");
        jsonObject.put("age", 30);*/
        Map<String,Object> dataMap=new HashMap<String, Object>();
        dataMap.put("id",23L);
        dataMap.put("username","mamade");
        dataMap.put("password","111111");
        dataMap.put("age","22");
        return ESUtil.addData(dataMap,"javaestest01","_doc","10103");
    }

    /**
     * @author 刘其佳
     * @description
     *      从数据库中查询数据
     *          如果查到了就存入es中，
     *          如果没有查到就返回null；
     * @param * param *:username
     * @date 2019/9/19
     * @return java.util.Map<java.lang.String,java.lang.Object>
     * @throws
     */
    public Map<String, Object> addDataBySql(String username){
        User user = userMapper.selectByUserName(username);
        if(user!=null){
            Map<String, Object> dataMap=new HashMap<String, Object>();
            dataMap.put("id",user.getId());
            dataMap.put("username",user.getUsername());
            dataMap.put("password",user.getPassword());
            dataMap.put("age",user.getAge());
            return  ESUtil.addData(dataMap,"javaestest01","_doc","3");
        }else{
            resultMap.put("result","未查询到数据");
            return resultMap;
        }
    }

    /**
     * @author 刘其佳
     * @description
     *      从数据库中查询所有数据，并放入到ES中
     *          其中用到了工具类中的对象转Map方法
     * @param * param *:index
     * @date 2019/9/19
     * @return java.util.Map<java.lang.String,java.lang.Object>
     * @throws
     */
    public Map<String, Object> addAllData(String index){
        List<User> userList = userMapper.selectAll();
        if(userList.size()>0){
            for (User user : userList) {
                Map<String , Object> mapObj=ESUtil.objectTurnMap(user);
                resultMap =  ESUtil.addData(mapObj,index,"test_type2");
            }
        }
        return resultMap;
    }

    /**
     * @author 刘其佳
     * @description
     *      删除数据
     * @param * param *:index
    * param *:type
    * param *:id
     * @date 2019/9/19
     * @return java.util.Map<java.lang.String,java.lang.Object>
     * @throws
     */
    public Map<String, Object> deleteDataById(String id){
        return ESUtil.deleteDataById("javaestest01", "_doc", id);
    }

    /**
     * @author 刘其佳
     * @description
     *      通过id进行查询数据
     *      （id:是ES给这一条数据所上的索引）
     *      searchDataById:一共需要传递四个参数
     *          index，type，id，fields
     *          fields:传递所要查询的字段（username,age）
     *          如果需要查询所有的字段直接传null
     * @param * param *:id
     * @date 2019/9/18
     * @return java.util.Map<java.lang.String,java.lang.Object>
     * @throws
     */
    public Map<String,Object> selectOneById(String id){
        return ESUtil.searchDataById("javaestest01","_doc",id,null);
    }

    /**
     * @author 刘其佳
     * @description
     *      查询所有的数据
     *          index
     *          type
     *          QueryBuilder：定义了查询条件（是全部查询 还是模糊查询 还是分页查询。。。。）
     *          size：所要查询出的条数
     *          field：所查询的字段（如果查询所有就直接写null）
     *          sortField:id，age...（根据字段进行排序，如果不需要设置则传null）
     *          highlightField:把搜索关键字进行高亮显示（如果不需要则传null）
     * @param * param *:
     * @date 2019/9/18
     * @return java.util.List<java.util.Map<java.lang.String,java.lang.Object>>
     * @throws
     */
    public List<Map<String,Object>> selectAll(){
        //1、创建QueryBuilder对象(BoolQueryBuilder是Builder的实现类)
        BoolQueryBuilder boolQueryBuilder= QueryBuilders.boolQuery();
        //2、创建所要搜索到额条件（查询所有数据）
        MatchAllQueryBuilder matchAllQueryBuilder=QueryBuilders.matchAllQuery();
        //3、把搜索的条件放入到BoolQueryBuilder中
        BoolQueryBuilder must = boolQueryBuilder.must(matchAllQueryBuilder);
        //4、返回
        return ESUtil.searchListData("javaestest01","_doc",must,100,null,null,null);
    }

    /**
     * @author 刘其佳
     * @description
     *      模糊查询
     *      在ES中默认如果单词之间没有连接符就会被当成一个单词
     *      例如zhangsan  就会被默认为一个词
     *      如果需要进行模糊匹配 在ES中必须要要使用连字符（_ - =.....）
     *      因为ES的分词器做的不够好，尤其是中文（必须要整合自己的分词器（IK），如果做得是职业搜索（用的最多的是搜狗））
     *      IK分词器集成很简单，不需要任何配置
     *          IK分词器：
     *              在ES的服务器，在plugins目录中创建IK文件夹（一定要大写）
     *              把IK分词器解压在IK目录中
     *              再次对ES文件夹进行授权
     * @param * param *:
     * @date 2019/9/19
     * @return java.util.List<java.util.Map<java.lang.String,java.lang.Object>>
     * @throws
     */
    public List<Map<String,Object>> selectLikeAll(String username){
        //1、创建QueryBuilder对象
        BoolQueryBuilder boolQueryBuilder=QueryBuilders.boolQuery();
        //2、创建查询条件
        //  matchPhraseQuery有两个参数：
        //name：字段名字
        //text：所需要模糊匹配的值（也就是SQL语句中like后面所匹配的值）
//        MatchPhraseQueryBuilder matchPhraseQueryBuilder=QueryBuilders.matchPhraseQuery("username","zhang");
        MatchPhraseQueryBuilder matchPhraseQueryBuilder=QueryBuilders.matchPhraseQuery("username",username);
        //3、把查询条件放到BoolQueryBuilder对象中
        BoolQueryBuilder must=boolQueryBuilder.must(matchPhraseQueryBuilder);
        return ESUtil.searchListData("javaestest01","_doc",must,10,null,null,"username");
    }

//    public void testQueryByStr(){
//        try {
//            String searchStr = "陈夏天u马立,@45";
//            QueryStringQueryBuilder builder = new QueryStringQueryBuilder(searchStr);
//
//            //  重点是下面这行代码
//            builder.analyzer("myanalyzer").field("username").field("password").field("age");
//            Iterable<User> search = userRepository.search(builder);
//            Iterator<User> iterator = search.iterator();
//            while (iterator.hasNext()){
//                System.out.println("---> 匹配数据： "+iterator.next());
//            }
//        }catch (Exception e){
//            System.out.println("---> 异常信息 "+e);
//        }
//    }


}
