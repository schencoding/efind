package com.hp.hplc.index;

import java.io.Serializable;
import java.rmi.Naming;
import java.util.Date;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.hp.hplc.indeximpl.iplookup.IPLookupIndex;
import com.hp.hplc.indeximpl.iplookup.IPLookupService;
import com.hp.hplc.indeximpl.iplookup.IPRange;

/**
 * Client end of IP lookup index.
 * 
 * @author Ma Dongzhe (mdzfirst@gmail.com)
 * @date 2012-5-28
 */
public class IPLookupIndexAccessor extends __IndexAccessor implements Serializable {
	private static final long serialVersionUID = 2731178083701152906L;

	public IPLookupIndexAccessor(String url) {
		super(url);
	}

	@Override
	public Vector<Writable> get(Writable key) {
		Vector<Writable> v = new Vector<Writable>();
		
		try {
			String str = ((Text) key).toString();
			int ip, temp, i;
			ip = temp = 0;
			for (i = 0; i < str.length(); i++) {
				char c = str.charAt(i);
				if (c == '.') {
					ip = (ip << 8) | temp;
					temp = 0;
					continue;
				}
				temp = temp * 10 + (c - '0');
			}
			ip = (ip << 8) | temp;
			
			IPLookupService service = (IPLookupService) Naming.lookup(IPLookupIndex.URL);
			IPRange range = service.get(ip);
			if (range != null && range.getStart() <= ip && ip <= range.getEnd())
				v.add(new Text(range.getInfo()));
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		return (v);
	}

	public static void main(String[] args) {
		IPLookupIndexAccessor index = new IPLookupIndexAccessor("Hello, world!");
		Scanner in = new Scanner(System.in);
		Date start, end;
		while (in.hasNext()) {
			String ip = in.next();
			start = new Date();
			Vector<Writable> v = index.get(new Text(ip));
			end = new Date();
			for (int i = 0; i < v.size(); i++)
				System.out.println(((Text) v.get(i)).toString());
			System.out.println("Response time: " + (end.getTime() - start.getTime()) / 1000.0 + " sec");
			System.out.println("----");
		}
	}
	
	@Override
	public Class<? extends Writable> getKeyClass() {
		return Text.class;
	}

	@Override
	public Class<? extends Writable> getValueClass() {
		return Text.class;
	}
}
