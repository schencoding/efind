package com.hp.hplc.expr.knnj;

import java.io.Serializable;
import java.rmi.Naming;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.hp.hplc.index.__IndexAccessor;
import com.hp.hplc.indeximpl.iplookup.IPLookupIndex;
import com.hp.hplc.indeximpl.iplookup.IPLookupService;
import com.hp.hplc.indeximpl.iplookup.IPRange;
import com.hp.hplc.rtree.service.RTreeServer;
import com.hp.hplc.rtree.service.RTreeService;
import com.hp.hplc.rtree.service.RTreeUtil;
import com.hp.*;


public class KnnIndexAccessor extends __IndexAccessor implements Serializable{

	public KnnIndexAccessor(String url) {
		super(url);
		// TODO Auto-generated constructor stub
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -5349551372846220523L;

	@Override
	public Vector<Writable> get(Writable key) {
		Vector<Writable> v = new Vector<Writable>();
		
		String row = key.toString();
		String[] xy = row.split("\\t");
		float fx = Float.parseFloat(xy[0]);
		float fy = Float.parseFloat(xy[1]);
		int ix = (int) fx;
		int iy = (int) fy;
		int x = (int) (fx * 10000000.0);
		int y = (int) (fy * 10000000.0);
		int partId = RTreeUtil.getPartId(ix, iy);
		int nodeId = RTreeUtil.getNodeId(partId);
		int portId = RTreeUtil.getPortId(partId);
		String url = RTreeServer.getURL(""+nodeId, portId);
		try {
			System.out.println("URL: " + url);
			RTreeService service = (RTreeService) Naming.lookup(url);
			String res = service.knn(x, y, 10);
			v.add(new Text(res));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return v;
	}

	@Override
	public Class<? extends Writable> getKeyClass() {
		return Text.class;
	}

	@Override
	public Class<? extends Writable> getValueClass() {
		return Text.class;
	}
	
	

	
	
	@Override
	public boolean isPartitioned() {
		return true;
	}

	@Override
	public int getNumberOfPartitions() {
		return 33;
	}

	@Override
	public List<String>[] getPartitionLocations() {
		List<String>[] ans = new List [33];
		
		
		return ans;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
