package com.hp.hplc.translator;

import java.util.HashMap;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.ChainReducer;

import com.hp.hplc.indexoperator.SimpleIndexOperator;
import com.hp.hplc.indexoperator.ParallelIndexOperator;
import com.hp.hplc.jobconf.IndexJobConf;

public class OldPlan {

	private IndexJobConf indexJobConf;
	private HashMap<Integer, OldPlanTask> map = new HashMap<Integer, OldPlanTask>();

	public OldPlan(IndexJobConf indexJobConf) {
		this.indexJobConf = indexJobConf;
	}
	
	public void setPlanTask(int key, OldPlanTask task){
		map.put(key, task);
	}
	
	public OldPlanTask getPlanTask(int key){
		return map.get(key);
	}

	public static OldPlan getPlan(IndexJobConf indexJobConf) {
		OldPlan plan = new OldPlan(indexJobConf);
		int counter = 0;
		// Head index operator
		java.util.Iterator itHead = indexJobConf.getHeadIndexOperators()
				.iterator();
		boolean bNewJob = true;
		while (itHead.hasNext()) {
			Object idxOp = itHead.next();
			OldPlanTask taskA = new OldPlanTask(bNewJob, false, false, "ChainMapper.AllMap", null, null);
			plan.setPlanTask(counter, taskA);
			counter ++;
			bNewJob = false;
		}

		// Map
		OldPlanTask mapTask = new OldPlanTask(bNewJob, false,false, "ChainMapper.AllMap",null,null);
		plan.setPlanTask(counter, mapTask);
		counter ++;

		// Body index operator
		java.util.Iterator itBody = indexJobConf.getBodyIndexOperators()
				.iterator();
		while (itBody.hasNext()) {
			Object idxOp = itBody.next();
			
			OldPlanTask taskA = new OldPlanTask(bNewJob, false, false, "ChainMapper.AllMap",null, null);
			plan.setPlanTask(counter, taskA);
			counter ++;			
		}

		// Reduce
		OldPlanTask reduceTask = new OldPlanTask(bNewJob, false,false,  "ChainReducer.Reducer",null,null);
		plan.setPlanTask(counter, reduceTask);
		counter ++;	

		// Tail index operators
		java.util.Iterator itTail = indexJobConf.getTailIndexOperators()
				.iterator();
		while (itTail.hasNext()) {
			Object idxOp = itTail.next();

			OldPlanTask taskB = new OldPlanTask(bNewJob, false,false, "ChainReducer.Mapper.AllMap",null,null);
			plan.setPlanTask(counter, taskB);
			counter ++;	
		}

		return plan;
	}

	

}
