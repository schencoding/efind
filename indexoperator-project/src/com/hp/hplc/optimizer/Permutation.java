package com.hp.hplc.optimizer;

import java.util.ArrayList;
import java.util.List;

public class Permutation {

	private static List<Integer> list = new ArrayList<Integer>();

	private static int total;

	public static void perm(List<List<Integer>> results, List<Integer> list, List<Integer> removed) {

		int length = list.size();
		if (length == 1) {
			List<Integer> result = new ArrayList<Integer>();
			for (int value : removed) {
				result.add(value);
				//System.out.print(value + " ");
			}
			result.add(list.get(0));
			//System.out.println(list.get(0));
			results.add(result);
			total++;
		} else {
			for (int i = 0; i < length; i++) {
				List<Integer> temp = new ArrayList<Integer>();
				temp.addAll(list);
				temp.remove(i);

				List<Integer> holder = new ArrayList<Integer>();
				holder.addAll(removed);
				holder.add(list.get(i));

				perm(results, temp, holder);

			}
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		list.add(1);
		list.add(2);
		list.add(3);
		list.add(4);
		list.add(5);
		
		List<List<Integer>> results = new ArrayList<List<Integer>>();

		perm(results, list, new ArrayList<Integer>());

		System.out.println("total: " + total);
		
		for (List<Integer> list : results) {
			for(int value: list){
				System.out.print(value + " ");
			}
			System.out.println();
		}
		
	}
}
