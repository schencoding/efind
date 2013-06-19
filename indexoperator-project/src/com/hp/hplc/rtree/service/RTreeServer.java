package com.hp.hplc.rtree.service;

import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;





public class RTreeServer {
	
	//public static String RTREE_SERVER_URL_PREFIX = "rmi://16.158.159.160:12345/RTreeServer";
	public static String RTREE_SERVER_URL_PREFIX = "rmi://15.154.147.";
	//public static String RTREE_SERVER_URL_PREFIX = "rmi://16.158.159.";
	public static String RTREE_SERVER_URL_SURFIX = "/RTreeServer";
	public static String RTREE_SERVER_NAME = "rtree_server_url";
	
	private String nodeId = "";
	private int port = 12345;
	
	public int getPort() {
		return port;
	}
	
	public void setPort(int port){
		this.port = port;
	}
	
	private void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}
	
	public String getServerURL(){
		return RTREE_SERVER_URL_PREFIX + nodeId + ":" + port + RTREE_SERVER_URL_SURFIX;
	}
	
	public static String getURL(String node, int myPort){
		return RTREE_SERVER_URL_PREFIX + node + ":" + myPort + RTREE_SERVER_URL_SURFIX;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if(args.length < 3){
			System.out.println("usage: nodeid port filename");
		}
		RTreeServer server = new RTreeServer();
		server.setNodeId(args[0]);
		int port  = Integer.parseInt(args[1]);
		server.setPort(port);
		try{
			RTreeServiceImpl service = new RTreeServiceImpl();			
			LocateRegistry.createRegistry(port);
			Naming.rebind(server.getServerURL(), service);
			service.load("" + port, args[2]);
			System.out.println("Service started.");
		}catch(Exception e){
			e.printStackTrace();
		}
				
	}

	

}
