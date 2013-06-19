package com.hp.hplc.rtree.service;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import rtree.Data;
import rtree.PPoint;
import rtree.RTree;
import rtree.SortedLinList;
import rtree.TreeCreation;
import rtree.UserInterface;

public class RTreeServiceImpl extends UnicastRemoteObject implements RTreeService {
	RTree rt = null;

	protected RTreeServiceImpl() throws RemoteException {
		super();
		/*TreeCreation tc = new TreeCreation("test.rtr", UserInterface.NUMRECTS, UserInterface.DIMENSION, UserInterface.BLOCKLENGTH, UserInterface.CACHESIZE);
		System.out.println("Finished loading data.");
		for (int i = 0; i < 100; i++) {
			PPoint p = new PPoint(2);
			p.data[0] = 0 + i * 100;
			p.data[1] = 0 + i * 100;

			System.out.println("Query point:" + p.data[0] + " " + p.data[1]);
			SortedLinList res = new SortedLinList();
			long start = System.currentTimeMillis();
			//tc.rt.point_query(p, res);
			//tc.rt.rangeQuery(p, 200, res);
			tc.rt.k_NearestNeighborQuery(p, 10, res);
			long end = System.currentTimeMillis();
			System.out.println("Time used: " + (end - start));
			System.out.println("# of data: " + res.get_num());
			System.out.println(res);
		}
		this.rt = tc.rt;
		//QueryProcessor qp = new QueryProcessor(tc.rt);
		//qp.start();*/

	}
	
	public void load(String treeFileName, String inputFileName){
		TreeCreation tc = new TreeCreation (treeFileName, inputFileName, UserInterface.DIMENSION, UserInterface.BLOCKLENGTH, UserInterface.CACHESIZE) ;
		this.rt = tc.rt;
		
		System.out.println("Finished loading data.");
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 2314558162355404023L;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	@Override
	public String knn(int x, int y, int k) throws RemoteException {
		PPoint p = new PPoint(2);
		p.data[0] = x;
		p.data[1] = y;
		SortedLinList list = new SortedLinList();
		rt.k_NearestNeighborQuery(p, k, list);
		String res = "";
		for(int i = 0; i < list.get_num(); i++){
			res += ((Data)list.get(i)).id + ",";
		}
		return res;
	}

	

}
