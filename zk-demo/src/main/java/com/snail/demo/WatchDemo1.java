package com.snail.demo;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

 
//监听目录
public class WatchDemo1 {
	
	static List<String> children = null;
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		

		ZooKeeper zkCli = new ZooKeeper("192.168.50.183:2181,192.168.50.184:2181,192.168.50.185:2181", 3000, new Watcher() {
				
				//监听回调
				public void process(WatchedEvent event) {
					System.out.println("正在监听中.....");
				}
			});
		
			//监听目录
			children = zkCli.getChildren("/", new Watcher() {
			
			public void process(WatchedEvent event) {
				System.out.println("监听路径为：" + event.getPath());
				System.out.println("监听的类型为：" + event.getType());
				System.out.println("数据被2货修改了！！！");
				
				for(String c:children) {
					System.out.println(c);
				}
			}
		});
			
			Thread.sleep(Long.MAX_VALUE);
		
	}
	
		
}
