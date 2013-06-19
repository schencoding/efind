package com.hp.hplc.expr.knnj;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import com.hp.hplc.indexoperator.util.K2V2Writable;



public class OsmPartitioner extends Partitioner<Text, K2V2Writable> {

	@Override
	public int getPartition(Text key, K2V2Writable value, int numOfPartitions) {
		String xy = key.toString();
		String[] xys = xy.split("\\t");
		float lo = Float.parseFloat(xys[0]);
		float la = Float.parseFloat(xys[1]);
		
		int loi = (int) (lo );
		int lai = (int) (la );
		
		int partId = osm.Partitioner.getPartitionId(loi, lai);
		return partId;
	}

	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	

}
