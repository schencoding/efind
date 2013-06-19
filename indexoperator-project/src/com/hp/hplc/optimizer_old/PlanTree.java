package com.hp.hplc.optimizer_old;

import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;

import com.hp.hplc.indexoperator.IndexOperator;
import com.hp.hplc.indexoperator.Pair;
import com.hp.hplc.indexoperator.__IndexOperator;
import com.hp.hplc.indexopimpl.ProfileIndexOperator;
import com.hp.hplc.jobconf.IndexJobConf;
import com.hp.hplc.mrimpl1.FrequencyReducer;
import com.hp.hplc.mrimpl1.TokenizerMapper;

public class PlanTree {

	private PlanNode root = new PlanNode();
	private IndexJobConf indexJobConf = null;

	public PlanTree(IndexJobConf indexJobConf) {
		this.indexJobConf = indexJobConf;
		this.buildTree();
	}
	
	public PlanNode getRoot(){
		return root;
	}

	public void buildTree() {

		Queue<PlanNode> queue = new LinkedList<PlanNode>();
		queue.add(root);
		PlanNode currNode = root;

		// Head index operator
		java.util.Iterator itHead = indexJobConf.getHeadIndexOperators().iterator();
		while (itHead.hasNext()) {
			Object idxOp = itHead.next();

			LinkedList<PlanNode> parentPlanNodes = new LinkedList<PlanNode>();
			while (!queue.isEmpty()) {
				parentPlanNodes.add(queue.poll());
			}

			for (int i = 0; i < parentPlanNodes.size(); i++) {
				currNode = parentPlanNodes.get(i);
				if (idxOp instanceof IndexOperator) {
					for (int planId = 1; planId <= 5; planId++) {
						IndexTaskPlan indexTask = new IndexTaskPlan((IndexOperator) idxOp, planId);
						PlanNode pNode = new PlanNode(indexTask);

						currNode.addPlanNode(pNode);
						queue.add(pNode);
					}
				}
			}

		}

		// Map
		if (indexJobConf.getMapperClass() != null) {
			try {
				Mapper mapper = indexJobConf.getMapperClass().getConstructor().newInstance();
				LinkedList<PlanNode> parentPlanNodes = new LinkedList<PlanNode>();
				while (!queue.isEmpty()) {
					parentPlanNodes.add(queue.poll());
				}

				for (int i = 0; i < parentPlanNodes.size(); i++) {
					currNode = parentPlanNodes.get(i);
					UserMapTaskPlan mapperTask = new UserMapTaskPlan(mapper);
					PlanNode pNode = new PlanNode(mapperTask);
					currNode.addPlanNode(pNode);
					queue.add(pNode);
				}
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NoSuchMethodException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			return;
		}

		// Body index operator
		java.util.Iterator itBody = indexJobConf.getBodyIndexOperators().iterator();
		while (itBody.hasNext()) {
			Object idxOp = itBody.next();

			LinkedList<PlanNode> parentPlanNodes = new LinkedList<PlanNode>();
			while (!queue.isEmpty()) {
				parentPlanNodes.add(queue.poll());
			}

			for (int i = 0; i < parentPlanNodes.size(); i++) {
				currNode = parentPlanNodes.get(i);
				if (idxOp instanceof IndexOperator) {
					for (int planId = 1; planId <= 5; planId++) {
						IndexTaskPlan indexTask = new IndexTaskPlan((IndexOperator) idxOp, planId);
						PlanNode pNode = new PlanNode(indexTask);

						currNode.addPlanNode(pNode);
						queue.add(pNode);
					}
				}
			}
		}

		// Reduce
		if (indexJobConf.getReducerClass() != null) {
			try {
				Reducer reducer = (Reducer) indexJobConf.getReducerClass().getConstructor().newInstance();
				LinkedList<PlanNode> parentPlanNodes = new LinkedList<PlanNode>();
				while (!queue.isEmpty()) {
					parentPlanNodes.add(queue.poll());
				}

				for (int i = 0; i < parentPlanNodes.size(); i++) {
					currNode = parentPlanNodes.get(i);
					for (int planId = 1; planId <= 2; planId++) {
						UserReducerTaskPlan reducerTask = new UserReducerTaskPlan(reducer, planId);
						PlanNode pNode = new PlanNode(reducerTask);
						currNode.addPlanNode(pNode);
						queue.add(pNode);
					}
				}

			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NoSuchMethodException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			return;
		}

		// Tail index operators
		java.util.Iterator itTail = indexJobConf.getTailIndexOperators().iterator();
		while (itTail.hasNext()) {
			Object idxOp = itTail.next();

			LinkedList<PlanNode> parentPlanNodes = new LinkedList<PlanNode>();
			while (!queue.isEmpty()) {
				parentPlanNodes.add(queue.poll());
			}

			for (int i = 0; i < parentPlanNodes.size(); i++) {
				currNode = parentPlanNodes.get(i);
				if (idxOp instanceof IndexOperator) {
					for (int planId = 1; planId <= 5; planId++) {
						IndexTaskPlan indexTask = new IndexTaskPlan((IndexOperator) idxOp, planId);
						PlanNode pNode = new PlanNode(indexTask);

						currNode.addPlanNode(pNode);
						queue.add(pNode);
					}
				}
			}
		}
	}

	public void printPlanTree() {
		List<PlanNode> children = root.getChildren();
		printPlanTree(children);
	}

	public void printPlanTree(List<PlanNode> parents) {
		List<PlanNode> children = new LinkedList<PlanNode>();
		Iterator<PlanNode> it = parents.iterator();
		while (it.hasNext()) {
			PlanNode parent = it.next();
			parent.print();
			children.addAll(parent.getChildren());
		}
		
		System.out.println();
		System.out.println();
		System.out.println();
		
		if (children.size() > 0) {
			printPlanTree(children);
		} else {
			return;
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		IndexJobConf idxJobConf = new IndexJobConf();
		List<Pair<String, String>> list = new LinkedList<Pair<String, String>>();
		__IndexOperator idxOp1 = new ProfileIndexOperator();

		idxJobConf.addHeadIndexOperator(idxOp1);
		idxJobConf.addHeadIndexOperator(idxOp1);

		idxJobConf.setMapperClass(TokenizerMapper.class);

		idxJobConf.addBodyIndexOperator(idxOp1);
		idxJobConf.addBodyIndexOperator(idxOp1);

		idxJobConf.setReducerClass(FrequencyReducer.class);
		idxJobConf.addTailIndexOperator(idxOp1);
		idxJobConf.addTailIndexOperator(idxOp1);

		PlanTree planTree = new PlanTree(idxJobConf);
		planTree.printPlanTree();

	}

}
