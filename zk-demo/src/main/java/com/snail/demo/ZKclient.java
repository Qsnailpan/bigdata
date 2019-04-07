package com.snail.demo;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

 
public class ZKclient {
	
	private String connectString = "192.168.50.183:2181,192.168.50.184:2181,192.168.50.185:2181";
	
	private int sessionTimeout = 3000;
	ZooKeeper zkCli = null;
	
	//初始化客户端
	@Before
	public void init() throws IOException {
		zkCli = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			
			//回调监听
			public void process(WatchedEvent event) {
				//System.out.println(event.getPath() + "\t" + event.getState() + "\t" + event.getType());
				
				try {
					List<String> children = zkCli.getChildren("/", true);
					
					for(String c:children) {
						//System.out.println(c);
					}
					
				} catch (KeeperException e) {
					
					e.printStackTrace();
				} catch (InterruptedException e) {
					
					e.printStackTrace();
				}
			}
		});
	}
	
	//创建子节点
	@Test
	public void createZnode() throws KeeperException, InterruptedException {
		String path = zkCli.create("/bbq", "shaokao".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		
		System.out.println(path);
	}
	
	//获取子节点
	@Test
	public void getChild() throws KeeperException, InterruptedException {
		List<String> children = zkCli.getChildren("/", true);
		
		for(String c:children) {
			System.out.println(c);
		}
		
		Thread.sleep(Long.MAX_VALUE);
	}
	
	//删除节点
	@Test
	public void rmChildDate() throws KeeperException, InterruptedException {
//		byte[] data = zkCli.getData("/bbq", true, null);
//		System.out.println(new String(data));
		
		zkCli.delete("/bbq", -1);
	}
	
	//修改数据
	@Test
	public void setData() throws KeeperException, InterruptedException {
		zkCli.setData("/hunter", "17".getBytes(), -1);
	}
	
	//判断节点是否存在
	@Test
	public void testExist() throws KeeperException, InterruptedException {
		Stat exists = zkCli.exists("/hunter1", false);
		
		System.out.println(exists == null ? "not exists":"exist");
	}
}
