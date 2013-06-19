package com.hp.hplc.index;

import java.io.Serializable;

import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.jcraft.jsch.jce.Random;

public class __HashIndexAccessor extends __IndexAccessor implements Serializable {
	private static final long serialVersionUID = 4216488103275168903L;

	public __HashIndexAccessor(String url) {
		super(url);
		this.setKeyClass(Text.class);
		this.setValueClass(Text.class);
	}

	@Override
	public Vector<Writable> get(Writable key) {
		Vector<Writable> vect = new Vector<Writable>();
		int rand = (int) (Math.random()*10);
		String str = "";
		for(int i=0; i< rand; i++){
			str += (key+":" + i);
		}
		vect.add(new Text(str));
		try {
			Thread.sleep(200);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return vect;
	}
	
	@Override
	public boolean isPartitioned() {
		return true;
	}

	@Override
	public int getNumberOfPartitions() {
		return 6;
	}

	@Override
	public List<String>[] getPartitionLocations() {
		
		List<String>[] lists = new LinkedList[6];
		for(int i=0; i<6; i++){
			lists[i] = new LinkedList<String>();
			lists[i].add("hplcchina-HP-xw8600-Workstation");
		}
		
		return lists;
		
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
