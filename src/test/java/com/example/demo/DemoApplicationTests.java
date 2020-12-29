package com.example.demo;

import com.example.demo.bean.User;
import com.example.demo.bean.ESProperties;
import com.example.demo.service.UserService;
import com.example.demo.serviceImpl.UserServiceImpl;
import lombok.SneakyThrows;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;

import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;


import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;


@RunWith(SpringRunner.class)
@SpringBootTest
public class DemoApplicationTests {

	@Autowired
	UserService service;

	@Autowired
	private ESProperties esProperties;

	@Autowired
	private UserServiceImpl userService;


	@Test
	public void contextLoads() {
		User userBean = service.loginIn("zss","zss");
		System.out.println("该用户ID为：");
		System.out.println(userBean.getId());
	}

	@Test
	public void testConnectEs(){
		//1、创建TransportClient对象
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
	}

	@Test
	public void addIndex(){
		Map<String, Object> result = userService.createIndex("javaestest01");
		System.out.println(result);//2020-12-26 21:37:49.072 {msg=操作成功, code=200}
	}

	@Test
	public void deleteIndex(){
		Map<String, Object> result = userService.deleteIndex("javaestest01");
		System.out.println(result);//2020-12-26 21:37:49.072 {msg=操作成功, code=200}
	}

	@Test
	public void addData(){
		Map<String, Object> result = userService.addData();
		System.out.println(result);//2020-12-26 21:37:49.072 {msg=操作成功, code=200}
	}

	@Test
	public void addDataBySql(){
		Map<String, Object> result = userService.addDataBySql("ss");
		System.out.println(result);//2020-12-26 21:37:49.072 {msg=操作成功, code=200}
	}

	@Test
	public void deleteDataById(){
		Map<String, Object> result = userService.deleteDataById("10103");
		System.out.println(result);//2020-12-26 21:37:49.072 {msg=操作成功, code=200}
	}

	@Test
	public void selectOneById(){
		Map<String, Object> result = userService.selectOneById("10101");
		System.out.println(result);//2020-12-26 21:37:49.072 {msg=操作成功, code=200}
	}

	@Test
	public void selectAll(){
		List<Map<String,Object>> result = userService.selectAll();
		System.out.println(result);//2020-12-26 21:37:49.072 {msg=操作成功, code=200}
	}

	@Test
	public void selectLikeAll(){
		List<Map<String,Object>> result = userService.selectLikeAll("ss");
		System.out.println(result);//2020-12-26 21:37:49.072 {msg=操作成功, code=200}
	}

}
