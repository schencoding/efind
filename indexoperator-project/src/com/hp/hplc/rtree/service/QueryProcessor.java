package com.hp.hplc.rtree.service;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import rtree.Data;
import rtree.PPoint;
import rtree.RTree;
import rtree.SortedLinList;
import rtree.rectangle;



public class QueryProcessor extends Thread{
	
	public static final int QUERY_PROCESSOR_PORT = 56790;
	
	private RTree rtree = null;
	private ServerSocket socketServer = null;
	
	public QueryProcessor(RTree rtree){
		System.out.println("Query process server is running...");
		this.rtree = rtree;
		try {
			socketServer = new ServerSocket(QUERY_PROCESSOR_PORT);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	@Override
	public void run() {
		while (!this.isInterrupted()) {
			try {
				Socket socket = socketServer.accept();
				QueryProcessSocketServerTask socketServerTask = new QueryProcessSocketServerTask();
				socketServerTask.setSocket(socket);
				socketServerTask.start();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public class QueryProcessSocketServerTask extends Thread{
		Socket socket = null;

		@Override
		public void run() {
			InputStream is = null;
			OutputStream os = null; 
			
	
			try {
				while (true) {
					is = socket.getInputStream();
					ObjectInputStream ois = null;
					ois = new ObjectInputStream(is);

					ObjectOutputStream oos = null;
					os = socket.getOutputStream();
					oos = new ObjectOutputStream(os);
					
					
					String command = (String) ois.readObject();
					Integer count = (Integer) ois.readObject();
					Object[] param = new Object[count.intValue()];
					for (int i = 0; i < count.intValue(); i++) {
						param[i] = ois.readObject();
					}

					if (command.compareToIgnoreCase("nearest") == 0) {
						
						int dimension = (Integer)param[0];
						float x = (Float) param[1];
						float y = (Float) param[2];
						PPoint p = new PPoint(dimension);//(PPoint) param[0];
						p.data[0] = x;
						p.data[1] = y;
						int k = ((Integer) param[3]).intValue();
					
						System.out.println("Query point:" + p.data[0] + " " + p.data[1]);
						SortedLinList res = new SortedLinList();
						long start = System.currentTimeMillis();
						//tc.rt.point_query(p, res);
						//tc.rt.rangeQuery(p, 200, res);
						//tc.rt.k_NearestNeighborQuery(p, 10, res);
						rtree.k_NearestNeighborQuery(p, k, res);//.nearestSearch((Point)param[0], (Long)param[1], (Integer)param[2]);
						oos.writeObject(new Boolean(true));
						oos.writeObject(new Integer(res.get_num()));
						for(int j=0; j<res.get_num(); j++){
							oos.writeObject(Long.toString(((Data)res.get(j)).id));							
						}
					}
					
					oos.flush();
					// oos.close();
					os.flush();
				}
			}catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} 
		}
		
		public void setSocket(Socket socket){
			this.socket = socket;
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
