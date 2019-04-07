package com.snail.demo;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.jboss.netty.util.internal.SystemPropertyUtil;

 
//监听单节点内容
public class WatchDemo {
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		ZooKeeper zkCli = new ZooKeeper("192.168.50.183:2181,192.168.50.184:2181,192.168.50.185:2181", 3000, new Watcher() {
			
			//监听回调
			public void process(WatchedEvent event) {
				
			}
		});
		
		byte[] data = zkCli.getData("/hunter", new Watcher() {
			//监听的具体内容
			public void process(WatchedEvent event) {
				System.out.println("监听路径为：" + event.getPath());
				System.out.println("监听的类型为：" + event.getType());
				System.out.println("数据被2货修改了！！！");
			}
		}, null);
		
		System.out.println(new String(data));
		
		Thread.sleep(Long.MAX_VALUE);
	}
}
