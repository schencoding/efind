package com.hp.hplc.translator;

import java.util.HashMap;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.lib.ChainReducer;

import com.hp.hplc.indexoperator.SimpleIndexOperator;
import com.hp.hplc.indexoperator.ParallelIndexOperator;
import com.hp.hplc.jobconf.IndexJobConf;

public class OOldPlan {

	private IndexJobConf indexJobConf;
	private HashMap<Object, PlanTask> map = new HashMap<Object, PlanTask>();

	public OOldPlan(IndexJobConf indexJobConf) {
		this.indexJobConf = indexJobConf;
	}
	
	public void setPlanTask(Object key, PlanTask task){
		map.put(key, task);
	}
	
	public PlanTask getPlanTask(Object key){
		return map.get(key);
	}

	public static OOldPlan getPlan(IndexJobConf indexJobConf) throws InstantiationException, IllegalAccessException {
		OOldPlan plan = new OOldPlan(indexJobConf);
		int counter = 0;
		// Head index operator
		java.util.Iterator itHead = indexJobConf.getHeadIndexOperators()
				.iterator();
		boolean bNewJob = true;
		while (itHead.hasNext()) {
			Object idxOp = itHead.next();
			PlanTask taskA = new PlanTask(idxOp, 0);
			plan.setPlanTask(idxOp, taskA);
			counter ++;
			bNewJob = false;
		}

		// Map
		Class mapCls = indexJobConf.getMapperClass();
		Object mapper = mapCls.newInstance();
		PlanTask mapTask = new PlanTask(mapper, 0);
		plan.setPlanTask(mapper, mapTask);
		counter ++;

		// Body index operator
		java.util.Iterator itBody = indexJobConf.getBodyIndexOperators()
				.iterator();
		while (itBody.hasNext()) {
			Object idxOp = itBody.next();
			
			PlanTask taskA = new PlanTask(idxOp, 0);
			plan.setPlanTask(idxOp, taskA);
			counter ++;			
		}

		// Reduce
		Class redCls = indexJobConf.getReducerClass();
		Object reducer = redCls.newInstance();
		PlanTask reduceTask = new PlanTask(reducer, 0);
		plan.setPlanTask(reducer, reduceTask);
		counter ++;	

		// Tail index operators
		java.util.Iterator itTail = indexJobConf.getTailIndexOperators()
				.iterator();
		while (itTail.hasNext()) {
			Object idxOp = itTail.next();

			PlanTask taskB = new PlanTask(idxOp, 0);
			plan.setPlanTask(idxOp, taskB);
		}

		return plan;
	}

	

}
