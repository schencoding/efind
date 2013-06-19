package com.hp.hplc.indeximpl.iplookup;

import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;

/**
 * Service end of IP lookup index.
 * 
 * @author Ma Dongzhe (mdzfirst@gmail.com)
 * @date 2012-5-28
 */
public class IPLookupIndex {
	private static String SRC = "/home/caoz/ip.csv";
	private static int PORT = 56789;
	public static String URL = "rmi://15.154.147.170:" + PORT + "/IPLookup";
	public static long delay = 0;
	
	public static void main(String[] args) {
		if (args.length == 1)
			delay = Long.valueOf(args[0]);
		System.out.println("Delay: " + delay + " ms");
		
		try {
			IPLookupServiceImpl service = new IPLookupServiceImpl(SRC);
			LocateRegistry.createRegistry(PORT);
			Naming.rebind(URL, service);
			System.out.println("Service started.");
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
