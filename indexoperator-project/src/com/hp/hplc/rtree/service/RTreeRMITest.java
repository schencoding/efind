package com.hp.hplc.rtree.service;

import java.io.IOException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.util.Random;

import rtree.PPoint;
import rtree.UserInterface;

public class RTreeRMITest {
	
	public static long time = 0;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		RTreeRMITest test = new RTreeRMITest();
		test.test();
		
	}
	
	public void test(){
		for(int i = 0; i< 10; i++){
			Client client = new Client(i);
			client.start();
		}
	}
	
	public class Client extends Thread{
		private int id = 0;
		
		public Client(int i){
			id = i;
		}

		@Override
		public void run() {
			try {
				RTreeService service = (RTreeService) Naming.lookup(RTreeServer.getURL("161", 10020));
				Random rand = new Random();
				for (int i = 0; i < 20000; i++) {
					int iy = rand.nextInt(UserInterface.MAXCOORD);// height
					int ix = rand.nextInt(UserInterface.MAXCOORD);// width
					PPoint p = new PPoint(2);
					p.data[0] = -850000000 + i * 100000;
					p.data[1] = 250000000 + i * 10000000;
					try {
						long start = System.currentTimeMillis();
						String res = service.knn(-850000000 + i * 10000000, 250000000 + i * 10000000, 10);
						long end = System.currentTimeMillis();
						//System.out.println("=========Query result for point " + p.data[0] + ", " + p.data[1] + "=========");

						System.out.println(res);

						//System.out.println("=============" + (end - start) + "========================================");
						time += (end-start);
						System.out.println("Total Time: " + time);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}

			} catch (IOException e) {
				e.printStackTrace();
			} catch (NotBoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

}
