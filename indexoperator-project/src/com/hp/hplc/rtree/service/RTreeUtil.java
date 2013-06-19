package com.hp.hplc.rtree.service;

import osm.Partitioner;

public class RTreeUtil {
	
	public static int getNodeId(int partId){
		return 161 + (partId % 10);
	}
	
	public static int getPortId(int partId){
		return 10020 + partId;
	}
	
	public static int getPartId(int lo, int lat){
		return Partitioner.getPartitionId(lo, lat);
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		for(int part = 0; part< 33; part++){
			System.out.println("" + part + " --->" + RTreeUtil.getNodeId(part) + ":" + RTreeUtil.getPortId(part));
		}
	}

}
