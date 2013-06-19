package com.hp.hplc.jobconf;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobInProgress;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.security.SecureShuffleUtils;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.mapreduce.split.JobSplitWriter;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

import com.hp.hplc.indexoperator.ParallelIndexOperator;
import com.hp.hplc.indexoperator.SimpleIndexOperator;
import com.hp.hplc.indexoperator.__IndexOperator;
import com.hp.hplc.metadata.InputAndIndexMapping;
import com.hp.hplc.metadata.PartitionAndIndexMapping;
import com.hp.hplc.mr.driver.MapInterMediateResultTest;
import com.hp.hplc.mrimpl1.FrequencyReducer;
import com.hp.hplc.optimizer.Optimizer;
import com.hp.hplc.optimizer.ParallelPlan;
import com.hp.hplc.plan.Plan;
import com.hp.hplc.plan.descriptor.IndexPreTaskDescriptor;
import com.hp.hplc.plan.descriptor.TaskDescriptor;
import com.hp.hplc.plan.exception.InvalidPlanException;
import com.hp.hplc.plan.exception.JobEmptyException;
import com.hp.hplc.plan.exception.JobNotFoundException;
import com.hp.hplc.plan.exception.MultipleReduceException;
import com.hp.hplc.plan.exception.TaskNotFoundException;
import com.hp.hplc.translator.JobTranslator;

public class IndexJobConf extends JobConf {
	
	public static int REEVALUATE_HTRESHOLD = 20;
	public static int FIXED_PLAN = 0;
	
	

	private LinkedList headIndexOperators = new LinkedList();
	private LinkedList bodyIndexOperators = new LinkedList();
	private LinkedList tailIndexOperators = new LinkedList();
	private String inputPartitionKey="";
	private Optimizer optimizer = new Optimizer(this);
	
	

	private String inputPartitionFunction="";

	public IndexJobConf() {
		super();
		// TODO Auto-generated constructor stub
	}

	public IndexJobConf(boolean loadDefaults) {
		super(loadDefaults);
		// TODO Auto-generated constructor stub
	}

	public IndexJobConf(Class exampleClass) {
		super(exampleClass);
		// TODO Auto-generated constructor stub
	}

	public IndexJobConf(Configuration conf, Class exampleClass) {
		super(conf, exampleClass);
		// TODO Auto-generated constructor stub
	}

	public IndexJobConf(Configuration conf) {
		super(conf);
		// TODO Auto-generated constructor stub
	}

	public IndexJobConf(Path config) {
		super(config);
		// TODO Auto-generated constructor stub
	}

	public IndexJobConf(String config) {
		super(config);
		// TODO Auto-generated constructor stub
	}

	public void addHeadIndexOperator(Object indexOperator) {
		headIndexOperators.add(indexOperator);
	}

	public LinkedList getHeadIndexOperators() {
		return headIndexOperators;
	}

	public void addBodyIndexOperator(Object indexOperator) {
		bodyIndexOperators.add(indexOperator);
	}

	public LinkedList getBodyIndexOperators() {
		return bodyIndexOperators;
	}

	public void addTailIndexOperator(Object indexOperator) {
		tailIndexOperators.add(indexOperator);
	}

	public LinkedList getTailIndexOperators() {
		return tailIndexOperators;
	}

	public void waitForCompletion(boolean b) throws Exception {		
		initMetastore();
		
		Plan plan = optimizer.optimize(FIXED_PLAN);

		runOriginalPlan(plan); 

	}
	
	public void runOriginalPlan(Plan plan) throws Exception {
		// plan.setOutputKeyClass(Text.class);
		// plan.setOutputValueClass(Text.class);
		plan.addInputPath(FileInputFormat.getInputPaths(this));
		plan.setOutputPath(FileOutputFormat.getOutputPath(this));
		plan.setInputFormat(this.getInputFormat().getClass());

		plan.print();

		List<JobConf> jobConfs = plan.translate();

		//Iterator<JobConf> it = jobConfs.iterator();

		String outputFolderStr = FileOutputFormat.getOutputPath(this).getName();
		String previousOutputStr = "";
		
		HashMap<Integer, Boolean> bMapReEvaluated = new HashMap<Integer, Boolean>();
		HashMap<Integer, Boolean> bReduceReEvaluated = new HashMap<Integer, Boolean>();

		long startTime = System.currentTimeMillis();
		
		int counter = -1;
		for(int jId = 0; jId < jobConfs.size(); jId++){	
			JobConf originalJobConf = jobConfs.get(jId);
			counter++;
			boolean bReEvaluated = false;	
			boolean bKilled = false;

			// Set input and output path
			FileOutputFormat.setOutputPath(originalJobConf, new Path(outputFolderStr + "/" + counter));
			previousOutputStr = outputFolderStr + "/" + (counter - 1);
			if (bMapReEvaluated.get(jId)==null || !bMapReEvaluated.get(jId)) {
				if (counter > 0) {
					FileInputFormat.addInputPath(originalJobConf, new Path(previousOutputStr));
				} else {
					Path[] paths = FileInputFormat.getInputPaths(this);
					for (int i = 0; i < paths.length; i++) {
						FileInputFormat.addInputPath(originalJobConf, paths[i]);
					}
				}
			}

			printJobInfo(originalJobConf);

			originalJobConf.setJobName("IndexOperator_Job" + counter);
			Job originalJob = new Job(originalJobConf);
			originalJob.submit();


			System.out.println("\n\n\nJob submitted: JOB " + originalJob.getJobName() + " with ID "
					+ originalJob.getJobID().toString());

			String strTaskTrackerHttp = "";
			String taskIdStr = "";
			List<Integer> finishedSplitIds = new ArrayList<Integer>();
			List<String> finishedMapsInfo = new ArrayList<String>();
			List<Integer> finishedReduces = new ArrayList<Integer>();

			int eventCounter = 0;
			int numFinishedMaps = 0;
			boolean bMap = true;
			while (!bKilled && !originalJob.isComplete()) {
				TaskCompletionEvent[] events = originalJob.getTaskCompletionEvents(eventCounter);
				eventCounter += events.length;

				if(Optimizer.metastore.isbEmpty())
					Optimizer.metastore.update(this, originalJob.getCounters(), plan, jId);
				/*else{
					if(REEVALUATE_HTRESHOLD > 2000000 ){
						Optimizer.metastore.update(this, originalJob.getCounters(), plan, jId);
					}
				}*/

				for (TaskCompletionEvent event : events) {

					System.out.println(event.toString());

					if (event.isMapTask() && event.getTaskStatus() == TaskCompletionEvent.Status.SUCCEEDED) {
						numFinishedMaps++;
						strTaskTrackerHttp = event.getTaskTrackerHttp();
						taskIdStr = event.getTaskId();
						finishedSplitIds.add(parseIntTaskId(taskIdStr));
						finishedMapsInfo.add(strTaskTrackerHttp);
						finishedMapsInfo.add(taskIdStr);
					} else if (!event.isMapTask() && event.getTaskStatus() == TaskCompletionEvent.Status.SUCCEEDED) {
						String str = event.getTaskId();
						String[] strs = str.split("_");
						if (strs[3].compareToIgnoreCase("r") == 0) {
							bMap = false;							
							finishedReduces.add(Integer.parseInt(strs[4])); //check if the id is located at 4?
						}
					} else {
					}
				}
				if (bMapReEvaluated.get(jId) == null || !bMapReEvaluated.get(jId).booleanValue()) {
					List<JobConf> newJobConfs = reEvaluate(numFinishedMaps, bMap, plan, jId, bReEvaluated, originalJob,originalJobConf);
					if (newJobConfs != null) {
						bReEvaluated = true;

						if(bMap){
							bMapReEvaluated.put(jId, true);
						}else{
							bReduceReEvaluated.put(jId, true);
						}
						originalJob.killJob();
						bKilled = true;
						System.out.println("Old job " + originalJob.getJobID().toString() + " was killed.");

						System.out.println("\n\n\n\n======== Going to run new plan========");
						
						int newCounter = counter+1;
						int numNewJobs = newJobConfs.size();
						for (int i = 0; i < newJobConfs.size(); i++) {
							if (bMap) {
								bMapReEvaluated.put(jId+1+i, true);
							} else {
								bReduceReEvaluated.put(jId+1+i, true);
							}
							// input path and skipped splits and maps
							if (i == 0) {
								Path[] paths = FileInputFormat.getInputPaths(originalJobConf);
								for (int j = 0; j < paths.length; j++) {
									FileInputFormat.addInputPath(newJobConfs.get(i), paths[j]);
								}
								if (bMap) {
									//Configure the skipped splits, map info
									newJobConfs.get(i).set(JobInProgress.SPLITS_TO_SKIP,
											composeSplitsToSkipStr(finishedSplitIds));
									String finishedMapsStr = composeStrFromList(finishedMapsInfo);
									newJobConfs.get(i).set(JobInProgress.SKIPPED_MAP_INFO, finishedMapsStr);
									
									newJobConfs.get(i).set(JobInProgress.B_SKIP_MAP_IN_MAP, "TRUE");
									
									if(i == numNewJobs-1){//The last job
										newJobConfs.get(i).set(JobInProgress.B_SKIP_MAP_IN_REDUCE, "FALSE");
									}else{
										newJobConfs.get(i).set(JobInProgress.B_SKIP_MAP_IN_REDUCE, "TRUE");
									}

									System.out.println("Skipped splits: " + composeSplitsToSkipStr(finishedSplitIds));
									System.out.println("Skipped map info: \n" + finishedMapsStr);
								} else {
									
									FileInputFormat.addInputPath(newJobConfs.get(i), new Path("EmptyInput"));
								}
							} else {
								FileInputFormat.addInputPath(newJobConfs.get(i), new Path(previousOutputStr));
								if (bMap) {
									if(i == numNewJobs-1){//The last job
										newJobConfs.get(i).set(JobInProgress.SPLITS_TO_SKIP,
												composeSplitsToSkipStr(finishedSplitIds));
										newJobConfs.get(i).set(JobInProgress.B_APPEND_MAP_IN_REDUCE, "TRUE");
										String finishedMapsStr = composeStrFromList(finishedMapsInfo);
										newJobConfs.get(i).set(JobInProgress.SKIPPED_MAP_INFO, finishedMapsStr);
										
										newJobConfs.get(i).set(JobInProgress.B_SKIP_MAP_IN_MAP, "FALSE");
									}
								}
							}
							// output path
							FileOutputFormat.setOutputPath(newJobConfs.get(i), new Path(outputFolderStr + "/" + newCounter));
							previousOutputStr = outputFolderStr + "/" + (newCounter);

							jobConfs.add(jId+1+i, newJobConfs.get(i));
							newCounter++;
						}
						/*if(!bMap){
							if(jobConfs.size()>=jId+newJobConfs.size()){
								JobConf nextOriginalJobConf = jobConfs.get(jId+newJobConfs.size());
								FileInputFormat.addInputPath(nextOriginalJobConf, new Path(outputFolderStr + "/" + counter));
							}
						}*/
						
					}
				}
				
				Thread.sleep(2 * 1000);
			}
			long currentTime = System.currentTimeMillis();
			
			System.out.println("\n\n\n");
			System.out.println("Time used untile now = " + (currentTime - startTime)/1000 );
			System.out.println("========Counters for Job: " + originalJob.getJobID().toString() + "========");
			System.out.println(originalJob.getCounters().toString());
			System.out.println("\n\n\n");
			//System.out.println("Job " + originalJob.getJobID().toString() + " finished.");

			//Optimizer.metastore.print();
			Optimizer.metastore.save();
		}
	}

	private List<JobConf> reEvaluate(int numFinished, boolean bMap, Plan plan, int ojId, boolean bReEvaluated, Job originalJob, JobConf originalJobConf) throws Exception {
		List<JobConf> jobConfs = null;
		boolean bAdjustPlan = true;
		
		/*System.out.println("----------reEvaluatePlan---------");
		System.out.println("\tnumFinished: " + numFinished);
		System.out.println("\tbMap: " + bMap);
		System.out.println("\tbReEvaluated: " + bReEvaluated);
		System.out.println("\tojId: " + ojId);*/
		
		if (bAdjustPlan && numFinished >= REEVALUATE_HTRESHOLD && !bReEvaluated) {
			Plan newPlan = optimizer.reEvaluate(plan, ojId, bMap, optimizer.numOfRecords);
			if(newPlan != null){
				newPlan.setInputFormat(originalJobConf.getInputFormat().getClass());
				newPlan.addInputPath(FileInputFormat.getInputPaths(originalJobConf));
				newPlan.setOutputPath(FileOutputFormat.getOutputPath(this));
				int numReudcers = this.getNumReduceTasks();
				newPlan.setNumOfReduces(numReudcers);
				jobConfs = newPlan.translate();
			}
			/*Vector<TaskDescriptor> descs = plan.getJob(ojId);
			Vector<__IndexOperator> idxOps = getIdxOps(descs);			
			
			if(newPlan != null){
				jobConfs = new JobConf[1];
				jobConfs[0] = plan.translate().get(0); //Dangerous!!! just use the old plan for test
			}*/
		}
		return jobConfs;
	}

	

	public void runNewPlan(int numFinishedMaps, boolean bReEvaluated, Plan plan, Job originalJob, int counter,
			List<Integer> finishedSplitIds, List<String> finishedMapsInfo, boolean bMap) throws Exception {
		boolean bAdjustPlan = true;
		if (bAdjustPlan && numFinishedMaps >= 2 && !bReEvaluated) {
			Plan newPlan = optimizer.reEvaluate(plan, counter, bMap, optimizer.numOfRecords);
			bReEvaluated = true;
			if (newPlan != null) {
				originalJob.killJob();
				System.out.println("Old job " + originalJob.getJobID().toString() + "was killed.");				
				
				System.out.println("\n\n\n\n======== New plan generated.========");				

				newPlan.setOutputKeyClass(Text.class);
				newPlan.setOutputValueClass(Text.class);
				newPlan.addInputPath(FileInputFormat.getInputPaths(this));
				newPlan.setOutputPath(FileOutputFormat.getOutputPath(this));
				newPlan.setInputFormat(this.getInputFormat().getClass());
				
				System.out.println("--------New plan--------");
				newPlan.print();

				
				String outputFolderStr = FileOutputFormat.getOutputPath(this).getName();
				String previousOutputStr = "";

				List<JobConf> newJobConfs = newPlan.translate();
				Iterator<JobConf> newIt = newJobConfs.iterator();
				int newCounter = -1;
				while (newIt.hasNext()) {
					newCounter++;
					JobConf newJobConf = newIt.next();
					
					// Set input and output path
					FileOutputFormat.setOutputPath(newJobConf, new Path(outputFolderStr + "/" + counter));
					previousOutputStr = outputFolderStr + "/" + (counter - 1);
					if (counter > 0) {
						FileInputFormat.addInputPath(newJobConf, new Path(previousOutputStr));
					} else {
						Path[] paths = FileInputFormat.getInputPaths(this);
						for (int i = 0; i < paths.length; i++) {
							FileInputFormat.addInputPath(newJobConf, paths[i]);
						}
					}

					printJobInfo(newJobConf);

					newJobConf.setJobName("IndexOperator_Job" + newCounter);
					//newJobConf.set(JobSplitWriter.INPUT_AND_INDEX_MAPPING, Metastore.gcomposeTestMapping());

					newJobConf.set(JobInProgress.SPLITS_TO_SKIP, composeSplitsToSkipStr(finishedSplitIds));
					String finishedMapsStr = composeStrFromList(finishedMapsInfo);
					newJobConf.set(JobInProgress.SKIPPED_MAP_INFO, finishedMapsStr);

					System.out.println("Skipped splits: " + composeSplitsToSkipStr(finishedSplitIds));
					System.out.println("Skipped map info: " + finishedMapsStr);

					String newOutputPathStr = FileOutputFormat.getOutputPath(this).toUri().getPath() + counter;
					FileOutputFormat.setOutputPath(newJobConf, new Path(newOutputPathStr));
					if (!bMap) {
						FileInputFormat.addInputPath(newJobConf, new Path("EmptyInput"));
					}

					Job newJob = new Job(newJobConf);
					newJob.submit();
					
					String strTaskTrackerHttp = "";
					String taskIdStr = "";
					List<Integer> newFinishedSplitIds = new ArrayList<Integer>();
					List<String> newFinishedMapsInfo = new ArrayList<String>();

					System.out.println("Job " + newJob.getJobName() + " with ID " + newJob.getJobID().toString());

					int newEventCounter = 0;
					while (!newJob.isComplete()) {
						//System.out.println(originalJob.getCounters().toString());
						Optimizer.metastore.update(this, newJob.getCounters(), plan, newCounter);

						TaskCompletionEvent[] newEvents = newJob.getTaskCompletionEvents(newEventCounter);
						newEventCounter += newEvents.length;
						for (TaskCompletionEvent newEvent : newEvents) {
							System.out.println(newEvent.toString());
							
							if (newEvent.isMapTask() && newEvent.getTaskStatus() == TaskCompletionEvent.Status.SUCCEEDED) {
								numFinishedMaps++;
								strTaskTrackerHttp = newEvent.getTaskTrackerHttp();
								taskIdStr = newEvent.getTaskId();
								newFinishedSplitIds.add(parseIntTaskId(taskIdStr));
								newFinishedMapsInfo.add(strTaskTrackerHttp);
								newFinishedMapsInfo.add(taskIdStr);

								Thread.sleep(5 * 1000);
							} else if (!newEvent.isMapTask() && newEvent.getTaskStatus() == TaskCompletionEvent.Status.SUCCEEDED) {
								bMap = false;
								String str = newEvent.getTaskId();
								String[] strs = str.split("_");
								if (strs[3].compareToIgnoreCase("r") == 0) {
									//originalJob.killJob();
									//should add finished reduces here
								}
							} else {
							}
						}
						runNewPlan(numFinishedMaps, bReEvaluated, plan, originalJob, counter, finishedSplitIds,
								finishedMapsInfo, bMap);
						Thread.sleep(10 * 1000);
					}
					System.out.println("Job " + newJob.getJobID().toString() + " finished.");
					
					Optimizer.metastore.save();
				}
			}
		} else {

		}
	}
	
	private void printJobInfo(JobConf jobConf) {
		System.out.println("\n\n\n---------------Job info-------------");
		System.out.println("InputFormat: " + jobConf.getInputFormat());
		System.out.println("Outputformat: " + jobConf.getOutputFormat());
		System.out.println("OutputPath: " + FileOutputFormat.getOutputPath(jobConf).toString());
		System.out.println("NumReduceTasks: " + jobConf.getNumReduceTasks());
		String strSplitToSkip = jobConf.get(JobInProgress.SPLITS_TO_SKIP);
		System.out.println("Splits to skip: " + strSplitToSkip);
		System.out.println("Map Class: " + jobConf.getMapperClass().getName());
		Class reduceClass = jobConf.getReducerClass();
		if(reduceClass != null)
			System.out.println("Reduce Class" + reduceClass.getName());
		
		String inputAndIndexMapStr = jobConf.get(JobSplitWriter.INPUT_AND_INDEX_MAPPING);
		if(inputAndIndexMapStr != null){
			System.out.println("Input and index Mapping: " + inputAndIndexMapStr);
		}
		
		String finishedMapsStr = jobConf.get(JobInProgress.SKIPPED_MAP_INFO);
		System.out.println(JobInProgress.SKIPPED_MAP_INFO + ": " + finishedMapsStr);
		String bSkipMapInMap = jobConf.get(JobInProgress.B_SKIP_MAP_IN_MAP);
		System.out.println(JobInProgress.B_SKIP_MAP_IN_MAP + ": " + bSkipMapInMap);		
		String bAppendMapInReduce = jobConf.get(JobInProgress.B_APPEND_MAP_IN_REDUCE);
		System.out.println(JobInProgress.B_APPEND_MAP_IN_REDUCE + ": " + bAppendMapInReduce);
		
		
		System.out.println("\n\n\n");
		
		
	}

	private void initMetastore() {
		java.util.Iterator itHead = this.getHeadIndexOperators().iterator();
		while (itHead.hasNext()) {
			Object obj = itHead.next();
			if (obj instanceof __IndexOperator) {
				__IndexOperator idxOp = (__IndexOperator) obj;
				Optimizer.metastore.initIdxOpMeta(idxOp);
			} else {
				System.err.println("Unkown index operator!");
			}
		}
		
		if(this.getMapperClass() != null){
			Optimizer.metastore.initUserMRMeta(this.getMapperClass().getName());
		}

		// Index Operators between user map and user reduce
		java.util.Iterator itBody = this.getBodyIndexOperators().iterator();
		while (itBody.hasNext()) {
			Object obj = itBody.next();
			if (obj instanceof __IndexOperator) {
				__IndexOperator idxOp = (__IndexOperator) obj;
				Optimizer.metastore.initIdxOpMeta(idxOp);
			} else {
				System.err.println("Unkown index operator!");
			}
		}
		
		if(this.getReducerClass() != null){
			Optimizer.metastore.initUserMRMeta(this.getReducerClass().getName());
		}

		// Index operators after user reduce
		java.util.Iterator itTail = this.getTailIndexOperators().iterator();
		while (itTail.hasNext()) {
			Object obj = itTail.next();
			if (obj instanceof __IndexOperator) {
				__IndexOperator idxOp = (__IndexOperator) obj;
				Optimizer.metastore.initIdxOpMeta(idxOp);
			} else {
				System.err.println("Unkown index operator!");
			}
		}
		
	}

	public String composeSplitsToSkipStr(List<Integer> list){
		String str = "";
		
		Iterator<Integer> it = list.iterator();
		while(it.hasNext()){
			int splitId = it.next();
			str += splitId;
			if(it.hasNext()){
				str += ",";
			}
		}		
		return str;
	}
	
	public String composeStrFromList(List<String> list){
		String str = "";		
		Iterator<String> it = list.iterator();
		while(it.hasNext()){
			String splitId = it.next();
			str += splitId;
			if(it.hasNext()){
				str += ",";
			}
		}		
		return str;
	}
	
	
/*	public String composeTestMapping(){
		InputAndIndexMapping inim = new InputAndIndexMapping();
		
		inim.addMapping("file:/home/hplcchina/workspace/indexoperator-project/input/1.txt", "hplcchina-HP-xw8600-Workstation");
		inim.addMapping("file:/home/hplcchina/workspace/indexoperator-project/input/2.txt", "hplcchina-HP-xw8600-Workstation");
		inim.addMapping("file:/home/hplcchina/workspace/indexoperator-project/input/3.txt", "hplcchina-HP-xw8600-Workstation");
		inim.addMapping("file:/home/hplcchina/workspace/indexoperator-project/input/4.txt", "hplcchina-HP-xw8600-Workstation");
		inim.addMapping("file:/home/hplcchina/workspace/indexoperator-project/input/5.txt", "hplcchina-HP-xw8600-Workstation");
		inim.addMapping("file:/home/hplcchina/workspace/indexoperator-project/EmptyInput/empty", "hplcchina-HP-xw8600-Workstation");
		
		//inim.addMapping("hdfs://localhost:9000/user/hplcchina/input/1.txt", "hplcchina-HP-xw8600-Workstation");
		//inim.addMapping("hdfs://localhost:9000/user/hplcchina/input/2.txt", "hplcchina-HP-xw8600-Workstation");
		//inim.addMapping("hdfs://localhost:9000/user/hplcchina/input/3.txt", "hplcchina-HP-xw8600-Workstation");
		//inim.addMapping("hdfs://localhost:9000/user/hplcchina/input/4.txt", "hplcchina-HP-xw8600-Workstation");
		//inim.addMapping("hdfs://localhost:9000/user/hplcchina/input/5.txt", "hplcchina-HP-xw8600-Workstation");
		//inim.addMapping("hdfs://localhost:9000/user/hplcchina/EmptyInput/empty", "hplcchina-HP-xw8600-Workstation");
		
		
		return inim.pack();
	}*/
	
	public String composePartitionAndIndexMappingStr() {
		PartitionAndIndexMapping paim = new PartitionAndIndexMapping();

		paim.addMapping(0, "hplcchina-HP-xw8600-Workstation");
		paim.addMapping(1, "hplcchina-HP-xw8600-Workstation");
		paim.addMapping(2, "hplcchina-HP-xw8600-Workstation");

		return paim.pack();
	}
	
	public int parseIntTaskId(String str) {
		String[] parts = str.split("_");
		return Integer.parseInt(parts[4]);
	}
	
	public String getInputPartitionKey() {
		return inputPartitionKey;
	}

	public void setInputPartitionKey(String inputPartitionKey) {
		this.inputPartitionKey = inputPartitionKey;
	}

	public String getInputPartitionFunction() {
		return inputPartitionFunction;
	}

	public void setInputPartitionFunction(String inputPartitionFunction) {
		this.inputPartitionFunction = inputPartitionFunction;
	}

}
