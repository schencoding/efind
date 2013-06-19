package com.hp.hplc.translator;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.ChainReducer;

import com.hp.hplc.indexoperator.SimpleIndexOperator;
import com.hp.hplc.indexoperator.ParallelIndexOperator;
import com.hp.hplc.jobconf.IndexJobConf;

public class JobTranslator {

	public static void translate(List<JobConf> jobConfs,
			IndexJobConf indexJobConf) {
		OldPlan plan = OldPlan.getPlan(indexJobConf);
		int outputCounter = 0;
		int counter = 0;
		JobConf currentJobConf = null;

		// Head index operator
		java.util.Iterator itHead = indexJobConf.getHeadIndexOperators()
				.iterator();
		while (itHead.hasNext()) {
			Object idxOp = itHead.next();

			OldPlanTask planTask = plan.getPlanTask(counter);
			if (planTask.isbNewLeftJob()) {
				JobConf jobConf = createNewJobConf(indexJobConf,
						outputCounter++);
				jobConfs.add(jobConf);
				currentJobConf = jobConf;
			}

			if (planTask.getLeftAction().compareToIgnoreCase(
					"ChainMapper.AllMap") == 0) {
				if (idxOp instanceof SimpleIndexOperator) {
					JobConf mapAConf = new JobConf(false);
					translateIdxOp2Mapper((SimpleIndexOperator) idxOp,
							currentJobConf, mapAConf, true);
				} else if (idxOp instanceof ParallelIndexOperator) {
					JobConf mapAConf = new JobConf(false);
					translatedParaIdxOp2Mapper((ParallelIndexOperator) idxOp,
							currentJobConf, mapAConf, true);
				}

			} else if (planTask.getLeftAction().compareToIgnoreCase(
					"ChainReducer.Reducer") == 0) {

			} else if (planTask.getLeftAction().compareToIgnoreCase(
					"ChainReducer.Mapper") == 0) {

			} else {
				System.out.println("Error plan.");
			}

			counter++;
		}

		// Map
		if (indexJobConf.getMapperClass() == null) {
			return;
		}
		OldPlanTask mapPlanTask = plan.getPlanTask(counter);
		if (mapPlanTask.isbNewLeftJob()) {
			JobConf jobConf = createNewJobConf(indexJobConf, outputCounter++);
			jobConfs.add(jobConf);
			currentJobConf = jobConf;
		}

		if (mapPlanTask.getLeftAction().compareToIgnoreCase(
				"ChainMapper.AllMap") == 0) {
			JobConf mapConf = new JobConf(false);
			translateMapper2Mapper(indexJobConf.getMapperClass(), indexJobConf,
					currentJobConf, mapConf);

		} else if (mapPlanTask.getLeftAction().compareToIgnoreCase(
				"ChainReducer.Reducer") == 0) {

		} else if (mapPlanTask.getLeftAction().compareToIgnoreCase(
				"ChainReducer.Mapper") == 0) {

		} else {
			System.out.println("Error plan.");
		}

		counter++;

		// Body index operator
		java.util.Iterator itBody = indexJobConf.getBodyIndexOperators()
				.iterator();
		while (itBody.hasNext()) {
			Object idxOp = itBody.next();

			OldPlanTask planTask = plan.getPlanTask(counter);
			if (planTask.isbNewLeftJob()) {
				JobConf jobConf = createNewJobConf(indexJobConf,
						outputCounter++);
				jobConfs.add(jobConf);
				currentJobConf = jobConf;
			}

			if (planTask.getLeftAction().compareToIgnoreCase(
					"ChainMapper.AllMap") == 0) {
				if (idxOp instanceof SimpleIndexOperator) {
					JobConf mapAConf = new JobConf(false);
					translateIdxOp2Mapper((SimpleIndexOperator) idxOp,
							currentJobConf, mapAConf, true);
				} else if (idxOp instanceof ParallelIndexOperator) {
					JobConf mapAConf = new JobConf(false);
					translatedParaIdxOp2Mapper((ParallelIndexOperator) idxOp,
							currentJobConf, mapAConf, true);
				}

			} else if (planTask.getLeftAction().compareToIgnoreCase(
					"ChainReducer.Reducer") == 0) {

			} else if (planTask.getLeftAction().compareToIgnoreCase(
					"ChainReducer.Mapper") == 0) {

			} else {
				System.out.println("Error plan.");
			}

			counter++;

		}

		// Reduce
		if (indexJobConf.getReducerClass() == null) {
			return;
		}
		Class reducerClass = indexJobConf.getReducerClass();
		Class inputKeyClass = indexJobConf.getMapOutputKeyClass();
		Class inputValueClass = indexJobConf.getMapOutputValueClass();
		Class outputKeyClass = indexJobConf.getOutputKeyClass();
		Class outputValueClass = indexJobConf.getOutputValueClass();

		OldPlanTask reducePlanTask = plan.getPlanTask(counter);
		if (reducePlanTask.isbNewLeftJob()) {
			JobConf jobConf = createNewJobConf(indexJobConf, outputCounter++);
			jobConfs.add(jobConf);
			currentJobConf = jobConf;
		}

		if (reducePlanTask.getLeftAction().compareToIgnoreCase(
				"ChainMapper.AllMap") == 0) {

		} else if (reducePlanTask.getLeftAction().compareToIgnoreCase(
				"ChainReducer.Reducer") == 0) {
			JobConf reducerConf = new JobConf(false);

			ChainReducer.setReducer(currentJobConf, reducerClass,
					inputKeyClass, inputValueClass, outputKeyClass,
					outputValueClass, true, reducerConf);
			System.out.println("In reducer!");

		} else if (reducePlanTask.getLeftAction().compareToIgnoreCase(
				"ChainReducer.Mapper") == 0) {

		} else {
			System.out.println("Error plan in reducer.");
		}

		counter++;

		// Tail index operators
		java.util.Iterator itTail = indexJobConf.getTailIndexOperators()
				.iterator();
		while (itTail.hasNext()) {
			Object idxOp = itTail.next();

			OldPlanTask planTask = plan.getPlanTask(counter);
			if (planTask.isbNewLeftJob()) {
				JobConf jobConf = createNewJobConf(indexJobConf,
						outputCounter++);
				jobConfs.add(jobConf);
				currentJobConf = jobConf;
			}

			if (planTask.getLeftAction().compareToIgnoreCase(
					"ChainMapper.AllMap") == 0) {

			} else if (planTask.getLeftAction().compareToIgnoreCase(
					"ChainReducer.Reducer") == 0) {

			} else if (planTask.getLeftAction().compareToIgnoreCase(
					"ChainReducer.Mapper") == 0) {
				if (idxOp instanceof SimpleIndexOperator) {
					JobConf reduceAConf = new JobConf(false);
					translateIdxOp2Mapper((SimpleIndexOperator) idxOp,
							currentJobConf, reduceAConf, false);
				} else if (idxOp instanceof ParallelIndexOperator) {
					JobConf reduceAConf = new JobConf(false);
					translatedParaIdxOp2Mapper((ParallelIndexOperator) idxOp,
							currentJobConf, reduceAConf, false);
				}
			} else {
				System.out.println("Error plan.");
			}

			counter++;

		}
	}

	public static JobConf createNewJobConf(IndexJobConf indexJobConf,
			int counter) {
		JobConf jobConf = new JobConf();

		jobConf.setInputFormat(indexJobConf.getInputFormat().getClass());

		jobConf.setOutputKeyClass(indexJobConf.getOutputKeyClass());
		jobConf.setOutputValueClass(indexJobConf.getOutputValueClass());

		String outputPathStr = FileOutputFormat.getOutputPath(indexJobConf)
				.toUri().toString()
				+ "/" + counter;

		FileInputFormat.setInputPaths(jobConf,
				FileInputFormat.getInputPaths(indexJobConf));
		FileOutputFormat.setOutputPath(jobConf, new Path(outputPathStr));

		return jobConf;
	}

	private static void translatedParaIdxOp2Mapper(
			ParallelIndexOperator parIdxOp, JobConf jobConf, JobConf mapAConf,
			boolean isMapperSide) {
		mapAConf.set("indexOperator.className", parIdxOp.getClass().getName());

		ChainMapper.addMapper(jobConf, Mapper4ParallelIndexOperator.class,
				Writable.class, Writable.class, parIdxOp.getOutputKeyClass(),
				parIdxOp.getOutputValueClass(), true, mapAConf);

	}

	private static void translateMapper2Mapper(
			Class<? extends org.apache.hadoop.mapred.Mapper> mapperClass,
			JobConf indexJobConf, JobConf jobConf, JobConf mapConf) {
		Class mapOutputKeyClass = indexJobConf.getMapOutputKeyClass();
		System.out.println("Map Output Key Class: " + mapOutputKeyClass);

		Class mapOutputValueClass = indexJobConf.getMapOutputValueClass();
		System.out.println("Map Output Value Class: " + mapOutputValueClass);

		mapConf.set("mapper.mapperclassName", indexJobConf.getMapperClass()
				.getName());

		ChainMapper.addMapper(jobConf, Mapper4Mapper.class, Writable.class,
				Writable.class, mapOutputKeyClass, mapOutputValueClass, true,
				mapConf);

	}

	private static void translateIdxOp2Mapper(SimpleIndexOperator idxOp,
			JobConf jobConf, JobConf mapAConf, boolean isMapperSide) {
		mapAConf.set("indexOperator.className", idxOp.getClass().getName());
		mapAConf.set("indexOperator.args", idxOp.getIndexURL());
		mapAConf.set("indexOperator.accerrosClassName", idxOp
				.getIndexAccessor().getClass().getName());

		if (isMapperSide) {
			ChainMapper.addMapper(jobConf, Mapper4SimpleIndexOperator.class,
					idxOp.getInputKeyClass(), idxOp.getInputValueClass(),
					idxOp.getOutputKeyClass(), idxOp.getOutputValueClass(),
					true, mapAConf);
		} else {
			ChainReducer.addMapper(jobConf, Mapper4SimpleIndexOperator.class,
					idxOp.getInputKeyClass(), idxOp.getInputValueClass(),
					idxOp.getOutputKeyClass(), idxOp.getOutputValueClass(),
					true, mapAConf);
		}

	}

}
