package com.hp.hplc.optimizer_old;

import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

import com.hp.hplc.indexoperator.IndexOperator;
import com.hp.hplc.jobconf.IndexJobConf;
import com.hp.hplc.metadata.Metastore;
import com.hp.hplc.plan.Plan;

public class Optimizer {

	private Metastore metastore = new Metastore();

	public Plan optimize(IndexJobConf indexJobConf) {
		Plan plan = new Plan();

		PlanTree tree = new PlanTree(indexJobConf);

		Stack<PlanNode> stack = new Stack<PlanNode>();
		PlanNode root = tree.getRoot();
		stack.push(root);

		long minCost = Long.MAX_VALUE;
		List<PlanNode> pathWithMiniCost = new LinkedList<PlanNode>();

		List<PlanNode> currentPath = new LinkedList<PlanNode>();
		while (!stack.isEmpty()) {
			PlanNode pNode = stack.pop();
			currentPath.add(pNode);

			if (pNode.hasChildren()) {
				List<PlanNode> children = pNode.getChildren();
				stack.addAll(children);
			} else {
				long cost = computeCost(indexJobConf, currentPath);
				if (cost < minCost) {
					minCost = cost;
					pathWithMiniCost.clear();
					pathWithMiniCost.addAll(currentPath);
				}
			}
		}

		return plan;
	}

	private long computeCost(IndexJobConf indexJobConf, List<PlanNode> currentPath) {
		int length = currentPath.size();
		long currCost = 0;
		for (int i = 0; i < length; i++) {
			PlanNode currNode = currentPath.get(i);
			TaskPlan currTaskPlan = currNode.getTaskPlan();
			int planId = currTaskPlan.getPlanId();
			if (currTaskPlan instanceof IndexTaskPlan) {
				if (planId == 1) {		//chain local
					long indexAccessCost = 0;
					//ToDo: Compute index access cost
					
					currCost += indexAccessCost;
				} else if (planId == 2) { //chain index node
					long copySplitsCost = 0;
					//ToDo: Compute the cost of copying split to index node
					
					currCost += copySplitsCost;
				} else if (planId == 3) { //Map + chain index node
					long copyPreResultCost = 0;
					//ToDo: compute the cost of copying the pre-process result to index node
					
					currCost += copyPreResultCost;		
				} else if (planId == 4) { //MapReduce + chain local
					
					boolean bAlreadyPart = checkPrecedentPartition(currentPath, i);
					
					if (!bAlreadyPart) {
						long shuffleCost = 0;
						//ToDo: compute the shuffle cost of pre-process result
						
						long indexAccessCost = 0;
						//ToDo: Compute index access cost based on partition
						
						currCost += shuffleCost;
						currCost += indexAccessCost;						
					}else{
						return Long.MAX_VALUE;
					}
					
					
					
					
				} else if (planId == 5) { //MapReduce + chain index node
					boolean bAlreadyPart = checkPrecedentPartition(currentPath, i);
					if (!bAlreadyPart) {
						long shuffleAndCopyCost = 0;
						// ToDo: compute the shuffle cost of pre-process result
						// and cost of copying reduce result to index node

						currCost += shuffleAndCopyCost;
					}else{
						return Long.MAX_VALUE;
					}
					
				} else {

				}

			} else if (currTaskPlan instanceof UserMapTaskPlan) {
				currCost += 0;

			} else if (currTaskPlan instanceof UserReducerTaskPlan) {
				if (planId == 1) {
					long shuffleCost = 0;
					//ToDo: compute the shuffle cost of previous operator or mapper
					

					currCost += shuffleCost;
				} else if (planId == 2) {
					currCost += 0;

				} else {

				}
			} else {

			}

		}
		return 0;
	}

	private boolean checkPrecedentPartition(List<PlanNode> currentPath, int index) {
		PlanNode currNode = currentPath.get(index);
		IndexOperator idxOp = ((IndexTaskPlan)currNode.getTaskPlan()).getIndexOperator();
		for(int i =index-1; i<0; i--){
			
		}
		return false;
	}

	private long computeNodeCost(PlanNode currNode, List<PlanNode> currentPath) {
		// TODO Auto-generated method stub
		return 0;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
