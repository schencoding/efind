package com.hp.hplc.indeximpl.iplookup;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IPLookupService extends Remote {
	public IPRange get(int ip) throws RemoteException;
}
