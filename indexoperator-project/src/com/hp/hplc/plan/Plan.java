package com.hp.hplc.plan;

import java.util.Vector;
import java.util.List;
import java.util.Iterator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import java.io.IOException;

import java.lang.Integer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.ChainReducer;

import com.hp.hplc.plan.descriptor.*;

import com.hp.hplc.plan.exception.JobNotFoundException;
import com.hp.hplc.plan.exception.TaskNotFoundException;
import com.hp.hplc.plan.exception.JobEmptyException;
import com.hp.hplc.plan.exception.MultipleReduceException;
import com.hp.hplc.plan.exception.InvalidPlanException;
import com.hp.hplc.util.Pair;

/**
 * Execution plan.
 * 
 * @author Ma Dongzhe (mdzfirst@gmail.com)
 * @date 2012-4-12
 */
public class Plan {
	private static final String TEMP_DIR_ROOT = "hdfs://localhost/mdz-tmp/";
	private static int serialDirectoryID = 0;
	
	public static final String JOB_PARAMETER_NAME = "mdz-param";
	public static final String SPLIT_PARAMETER_NAME = "mdz-split";
	
	public static final boolean PASS_BY_VALUE = false;
	
	public static final Class<? extends Writable> DEFAULT_KEY_CLASS = LongWritable.class;
	public static final Class<? extends Writable> DEFAULT_VALUE_CLASS = Text.class;
	
	private Class<? extends InputFormat> inputFormat = TextInputFormat.class;
	private Class<? extends OutputFormat> outputFormat = TextOutputFormat.class;

	private Vector<Path> inputPath = null;
	private Path outputPath = null;
	
	private Vector<Vector<TaskDescriptor> > jobList = null;
	private Vector<TaskDescriptor> allTaskList = null;
	private int taskCounter = -1;
	private Vector<Integer> numOfReduces4Jobs = null;

	public Plan() {
		inputPath = new Vector<Path>();
		jobList = new Vector<Vector<TaskDescriptor> >();
		allTaskList = new Vector<TaskDescriptor>();
		numOfReduces4Jobs = new Vector<Integer>();
	}
	
	public Vector<TaskDescriptor> getJob(int index){
		return jobList.get(index);		
	}
	
	public void addJob(Vector<TaskDescriptor> v){
		jobList.add(v);
		numOfReduces4Jobs.add(2);
	}
	
	public void setAllTaskList(Vector<TaskDescriptor> v){
		allTaskList = v;
	}
	
	private static String getTempDirectory() {
		return (TEMP_DIR_ROOT + (serialDirectoryID++));
	}
	
	public void setInputFormat(Class<? extends InputFormat> inputFormat) {
		this.inputFormat = inputFormat;
	}
	
	public void setOutputFormat(Class<? extends OutputFormat> outputFormat) {
		this.outputFormat = outputFormat;
	}
	
	public void addInputPath(Path path) {
		inputPath.add(path);
	}
	
	public void addInputPath(Path[] paths) {
		for (int i = 0; i < paths.length; i++)
			addInputPath(paths[i]);
	}
	
	public void setOutputPath(Path path) {
		outputPath = path;
	}
	
	public void addJob() {
		jobList.add(new Vector<TaskDescriptor>());
		numOfReduces4Jobs.add(2);
	}

	public void addTask(TaskDescriptor task) throws JobNotFoundException {
		if (jobList.size() == 0)
			throw new JobNotFoundException();
		jobList.lastElement().add(task);
		allTaskList.add(task);
		
		taskCounter ++;
		task.setID(taskCounter);
	}
	
	public void setInputKeyClass(Class<? extends Writable> theClass)
		throws JobNotFoundException, TaskNotFoundException {
		if (jobList.size() == 0)
			throw new JobNotFoundException();
		if (allTaskList.size() == 0)
			throw new TaskNotFoundException();
		allTaskList.lastElement().setInputKeyClass(theClass);
	}
	
	public void setInputValueClass(Class<? extends Writable> theClass)
		throws JobNotFoundException, TaskNotFoundException {
		if (jobList.size() == 0)
			throw new JobNotFoundException();
		if (allTaskList.size() == 0)
			throw new TaskNotFoundException();
		allTaskList.lastElement().setInputValueClass(theClass);
	}
	
	public void setOutputKeyClass(Class<? extends Writable> theClass)
		throws JobNotFoundException, TaskNotFoundException {
		if (jobList.size() == 0)
			throw new JobNotFoundException();
		if (allTaskList.size() == 0)
			throw new TaskNotFoundException();
		allTaskList.lastElement().setOutputKeyClass(theClass);
	}

	public void setOutputValueClass(Class<? extends Writable> theClass)
		throws JobNotFoundException, TaskNotFoundException {
		if (jobList.size() == 0)
			throw new JobNotFoundException();
		if (allTaskList.size() == 0)
			throw new TaskNotFoundException();
		allTaskList.lastElement().setOutputValueClass(theClass);
	}

	public List<JobConf> translate() throws IOException,
		JobNotFoundException, TaskNotFoundException, JobEmptyException, MultipleReduceException,
		InvalidPlanException {
		
		
		
		List<JobConf> res = new Vector<JobConf>();
		int i, j;
		
		System.out.println();
		System.out.println("======== Execution details ========");
		System.out.println();
		System.out.println("Input format: " + inputFormat.getSimpleName());
		System.out.println("Output format: " + outputFormat.getSimpleName());
		System.out.println();
		for (i = 0; i < inputPath.size(); i++) {
			if (i == 0)
				System.out.print("Input paths: ");
			else
				System.out.print("             ");
			System.out.println(inputPath.get(i).toString());
		}
		System.out.println("Output path: " + outputPath.toString());
		System.out.println();
		for (i = 0; i < allTaskList.size(); i++)
			System.out.println(allTaskList.get(i).getDetails());
		System.out.println();

		
		if (inputPath == null || inputPath.size() == 0)
			throw new InvalidPlanException("Input path not specified");
		if (outputPath == null)
			throw new InvalidPlanException("Output path not specified");

		if (jobList.size() == 0)
			throw new JobNotFoundException();
		if (allTaskList.size() == 0)
			throw new TaskNotFoundException();
		
		// This behavior is not identical to that of Hadoop.
		TaskDescriptor lastTask = allTaskList.lastElement();
		if (lastTask.getOutputKeyClass() == null)
			lastTask.setOutputKeyClass(DEFAULT_KEY_CLASS);
		if (lastTask.getOutputValueClass() == null)
			lastTask.setOutputValueClass(DEFAULT_VALUE_CLASS);

		for (i = 1; i < allTaskList.size(); i++) {
			TaskDescriptor task = allTaskList.get(i);
			TaskDescriptor former = allTaskList.get(i - 1);
			if (task.getInputKeyClass() == null && former.getOutputKeyClass() != null)
				task.setInputKeyClass(former.getOutputKeyClass());
			if (task.getInputValueClass() == null && former.getOutputValueClass() != null)
				task.setInputValueClass(former.getOutputValueClass());
		}
		/*
		for (i = allTaskList.size() - 1; i >= 0; i--) {
			TaskDescriptor task = allTaskList.get(i);
			if (task.getOutputKeyClass() == null)
				task.setOutputKeyClass(allTaskList.get(i + 1).getInputKeyClass());
			if (task.getOutputValueClass() == null)
				task.setOutputValueClass(allTaskList.get(i + 1).getInputValueClass());
			if (task.getInputKeyClass() == null)
				task.setInputKeyClass(task.getOutputKeyClass());
			if (task.getInputValueClass() == null)
				task.setInputValueClass(task.getOutputValueClass());
		}*/
		
		for (i = 1; i < allTaskList.size(); i++) {
			TaskDescriptor former = allTaskList.get(i - 1);
			TaskDescriptor latter = allTaskList.get(i);
			if (former.getOutputKeyClass() != latter.getInputKeyClass())
				throw new IOException("Type mismatch in key from " + latter.toString() +
						": expected " + latter.getInputKeyClass().toString() +
						", recieved " + former.getOutputKeyClass().toString());
			if (former.getOutputValueClass() != latter.getInputValueClass())
				throw new IOException("Type mismatch in value from " + latter.toString() +
						": expected " + latter.getInputValueClass().toString() +
						", recieved " + former.getOutputValueClass().toString());
		}

		Vector<Path> input = this.inputPath;
		Class<? extends InputFormat> format = this.inputFormat;
		
		for (i = 0; i < jobList.size(); i++) {
			Vector<TaskDescriptor> taskList = jobList.get(i);
			
			if (taskList.size() == 0)
				throw new JobEmptyException();
			
			Vector<Vector<TaskDescriptor> > splits = new Vector<Vector<TaskDescriptor> >();
			int reduceIndex = -1;

			if (true) {
				/*
				 * TODO: Split taskList into ChainedMapper or ChainedReducer here.
				 *       At this moment, I chained everything.
				 */
				for (j = 0; j < taskList.size(); j++) {
					TaskDescriptor task = taskList.get(j);
					
					Vector<TaskDescriptor> split = new Vector<TaskDescriptor>();
					split.add(task);
					splits.add(split);
					
					if (task.getType() == TaskType.REDUCE) {
						if (reduceIndex == -1)
							reduceIndex = splits.size() - 1;
						else
							throw new MultipleReduceException();
					}
				}
			} else {
				/*
				 * TODO: Split taskList into ChainedMapper or ChainedReducer here.
				 *       At this moment, I do all operations serially.
				 */
				Vector<TaskDescriptor> split = new Vector<TaskDescriptor>();
				for (j = 0; j < taskList.size(); j++) {
					TaskDescriptor task = taskList.get(j);
					
					if (task.getType() == TaskType.REDUCE) {
						splits.add(split);
						split = new Vector<TaskDescriptor>();
					}

					split.add(task);
				}
				
				splits.add(split);
				
				if (splits.size() == 2)
					reduceIndex = 1;
				else if (splits.size() > 2)
					throw new MultipleReduceException();
			}
			
			Object[] param = {splits, reduceIndex};
			
			ByteArrayOutputStream ba = new ByteArrayOutputStream();
			ObjectOutputStream o = new ObjectOutputStream(ba);
			o.writeObject(param);
			String stringParam = ba.toString("ISO-8859-1");

			JobConf conf = new JobConf(Plan.class);
			
			// Pass task properties here.
			for (j = 0; j < taskList.size(); j++) {
				TaskDescriptor task = taskList.get(j);
				Iterator<Pair<String, String> > itr = task.getProperties().iterator();
				while (itr.hasNext()) {
					Pair<String, String> property = itr.next();
					if(property.first != null && property.second != null)
						conf.set(property.first, property.second);
				}
			}

			conf.set(JOB_PARAMETER_NAME, ParamHelper.encode(stringParam));
	
			//System.out.println("code length = " + ParamHelper.encode(stringParam).length());
			//System.out.println("code = " + ParamHelper.encode(stringParam));
			
			Iterator<Path> itr = input.iterator();
			while (itr.hasNext()) {
				itr.next();
				//FileInputFormat.addInputPath(conf, itr.next());
				conf.setInputFormat(format);
			}
			if (i + 1 < jobList.size()) {
				Path path = new Path(getTempDirectory());
				FileOutputFormat.setOutputPath(conf, path);
				
				input = new Vector<Path>();
				input.add(path);
				
				conf.setOutputFormat(SequenceFileOutputFormat.class);
				format = SequenceFileInputFormat.class;
				
			} else {
				FileOutputFormat.setOutputPath(conf, outputPath);
				conf.setOutputFormat(this.outputFormat);
			}
			
			conf.setNumReduceTasks(this.numOfReduces4Jobs.get(i));
			
			boolean reducerReached = false;
			for (j = 0; j < splits.size(); j++) {
				Vector<TaskDescriptor> split = splits.get(j);
				assert split.size() > 0;
				
				Class<? extends Writable> inputKeyClass = split.get(0).getInputKeyClass();
				Class<? extends Writable> inputValueClass = split.get(0).getInputValueClass();
				Class<? extends Writable> outputKeyClass = split.lastElement().getOutputKeyClass();
				Class<? extends Writable> outputValueClass = split.lastElement().getOutputValueClass();
				
				JobConf splitConf = new JobConf(false);
				splitConf.set(SPLIT_PARAMETER_NAME, String.valueOf(j));

				if (j == reduceIndex) {
					assert ! reducerReached;
					reducerReached = true;
					ChainReducer.setReducer(conf, ReduceWorker.class,
						inputKeyClass, inputValueClass, outputKeyClass, outputValueClass,
						PASS_BY_VALUE, splitConf);
				} else if (reducerReached) {
					ChainReducer.addMapper(conf, MapWorker.class,
						inputKeyClass, inputValueClass, outputKeyClass, outputValueClass,
						PASS_BY_VALUE, splitConf);
				} else {
					ChainMapper.addMapper(conf, MapWorker.class,
						inputKeyClass, inputValueClass, outputKeyClass, outputValueClass,
						PASS_BY_VALUE, splitConf);
				}
			}

			conf.setOutputKeyClass(taskList.lastElement().getOutputKeyClass());
			conf.setOutputValueClass(taskList.lastElement().getOutputValueClass());
			
			conf.setNumTasksToExecutePerJvm(-1);
			
			res.add(conf);
		}
		
		System.out.println();
		System.out.println("======== Execution details ========");
		System.out.println();
		System.out.println("Input format: " + inputFormat.getSimpleName());
		System.out.println("Output format: " + outputFormat.getSimpleName());
		System.out.println();
		for (i = 0; i < inputPath.size(); i++) {
			if (i == 0)
				System.out.print("Input paths: ");
			else
				System.out.print("             ");
			System.out.println(inputPath.get(i).toString());
		}
		System.out.println("Output path: " + outputPath.toString());
		System.out.println();
		for (i = 0; i < allTaskList.size(); i++)
			System.out.println(allTaskList.get(i).getDetails());
		System.out.println();

		return (res);
	}
	
	public void print(){
		Iterator<Vector<TaskDescriptor>> jobIt = this.jobList.iterator();
		while(jobIt.hasNext()){
			System.out.println("-------------Job------------");
			Vector<TaskDescriptor> job = jobIt.next();
			
			Iterator<TaskDescriptor> taskIt = job.iterator();
			while(taskIt.hasNext()){
				TaskDescriptor task = taskIt.next();
				task.print();
			}
		}
	}

	public static void main(String[] args) {
		Plan plan = new Plan();

		try {
			plan.addInputPath(new Path("hdfs://localhost/input"));
			plan.setOutputPath(new Path("hdfs://localhost/output"));
			
			plan.addJob();
			plan.addTask(new SystemMapTaskDescriptor(null, TaskType.MAP));
			plan.setInputKeyClass(LongWritable.class);
			plan.setInputValueClass(Text.class);
			plan.addTask(new SystemMapTaskDescriptor(null, TaskType.REDUCE));
			plan.addJob();
			plan.addTask(new SystemMapTaskDescriptor(null, TaskType.REDUCE));
			plan.setOutputKeyClass(Text.class);
			plan.setOutputValueClass(IntWritable.class);
			
			List<JobConf> confList = plan.translate();
			Iterator<JobConf> confItr = null;
			
			confItr = confList.iterator();
			while (confItr.hasNext()) {
				JobConf conf = confItr.next();
				
				String stringParam = ParamHelper.decode(conf.get(Plan.JOB_PARAMETER_NAME));		
				ByteArrayInputStream ba =
					new ByteArrayInputStream(stringParam.getBytes("ISO-8859-1"));
				ObjectInputStream o = new ObjectInputStream(ba);
				Object[] param = (Object[]) o.readObject();
				
				Vector<Vector<TaskDescriptor> > splits =
					(Vector<Vector<TaskDescriptor> >) param[0];
				int reduceIndex = ((Integer) param[1]).intValue();
				
				// System.out.println(splits);
				// System.out.println(reduceIndex);
				
				int i, j;
				System.out.println("New MapReduce job:");
				for (i = 0; i < splits.size(); i++) {
					Vector<TaskDescriptor> split = splits.get(i);
					System.out.println("New split:");
					for (j = 0; j < split.size(); j++) {
						TaskDescriptor task = split.get(j);
						System.out.println(task.toString() + task.getDetails());
					}
				}
			}
			
			confItr = confList.iterator();
			while (confItr.hasNext())
				JobClient.runJob(confItr.next());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void setNumOfReduces(int i) {
		this.numOfReduces4Jobs.set(this.jobList.size()-1, i);
		
	}
}
