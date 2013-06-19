package com.hp.hplc.rtree.service;

import java.io.IOException;
import java.util.Random;


import rtree.PPoint;
import rtree.UserInterface;

public class Querier {

	private RTreeClient client = null;

	public Querier(String host) {
		client = new RTreeClient(host, QueryProcessor.QUERY_PROCESSOR_PORT);
	}
	
	public void search(){
		int h = 15000000;
		int w = 6000000;
		int ix,iy,xx,xy;//mIn,maX
		Random rand = new Random();
		for(int i=0; i<20; i++){
			iy = rand.nextInt(UserInterface.MAXCOORD);// height
			ix = rand.nextInt(UserInterface.MAXCOORD);// width
			PPoint p = new PPoint(2);
			p.data[0] = 0 + i * 1000;
			p.data[1] = 0 + i * 1000;
			try {
				long start = System.currentTimeMillis();
				String[] abls = client.nearestSearch(p, 10);
				long end = System.currentTimeMillis();
				System.out.println("=========Query result for point "+ p.data[0] + ", " + p.data[1] +"=========");
				
				for(int j = 0; j < abls.length; j++){
					System.out.println(abls[j]);
				}
				
				System.out.println("=============" + (end-start) + "========================================");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		try {
			System.in.read();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Querier q = new Querier("localhost");
		q.search();
		q.stop();
	}

	private void stop() {
		client.stop();
		
	}

}
