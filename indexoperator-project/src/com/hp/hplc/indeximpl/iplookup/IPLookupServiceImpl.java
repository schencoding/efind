package com.hp.hplc.indeximpl.iplookup;

import java.io.FileReader;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Scanner;
import java.util.TreeMap;

public class IPLookupServiceImpl extends UnicastRemoteObject implements IPLookupService {
	private static final long serialVersionUID = -2561116246442786308L;
	private TreeMap<Integer, IPRange> m = null;
	
	public IPLookupServiceImpl(String src) throws RemoteException {
		super();
		
		try {
			int cnt = 0;
			Scanner in = new Scanner(new FileReader(src));
			m = new TreeMap<Integer, IPRange>();
			while (in.hasNext()) {
				String line = in.nextLine();
				int start, end, temp, i;
				start = end = temp = 0;
				for (i = 0; i < line.length(); i++) {
					char c = line.charAt(i);
					if (c == '.' || c == ',') {
						start = (start << 8) | temp;
						temp = 0;
						if (c == ',')
							break;
						continue;
					}
					temp = temp * 10 + (c - '0');
				}
				for (i++; i < line.length(); i++) {
					char c = line.charAt(i);
					if (c == '.' || c == ',') {
						end = (end << 8) | temp;
						temp = 0;
						if (c == ',')
							break;
						continue;
					}
					temp = temp * 10 + (c - '0');
				}
				IPRange r = new IPRange(start, end, line.substring(i + 1));
				m.put(start, r);
				cnt++;
			}
			System.out.println(cnt + " records loaded.");
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	@Override
	public IPRange get(int ip) throws RemoteException {
		try {
			if (IPLookupIndex.delay > 0)
				Thread.sleep(IPLookupIndex.delay);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		Integer key = m.floorKey(ip);
		if (key == null)
			return (null);
		return (m.get(key));
	}
}
