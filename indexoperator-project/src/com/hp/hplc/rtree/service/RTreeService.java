package com.hp.hplc.rtree.service;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface RTreeService extends Remote {
	public String knn(int x, int y, int k) throws RemoteException;
}
