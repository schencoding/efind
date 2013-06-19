package com.hp.hplc.metadata;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;

import com.hp.hplc.index.__IndexAccessor;
import com.hp.hplc.indexoperator.__IndexOperator;
import com.hp.hplc.indexopimpl.ProfileIndexOperator;
import com.hp.hplc.jobconf.IndexJobConf;
import com.hp.hplc.plan.IndexCounter;
import com.hp.hplc.plan.Plan;
import com.hp.hplc.plan.descriptor.IndexLookupTaskDescriptor;
import com.hp.hplc.plan.descriptor.IndexPostTaskDescriptor;
import com.hp.hplc.plan.descriptor.IndexPreTaskDescriptor;
import com.hp.hplc.plan.descriptor.TaskDescriptor;
import com.hp.hplc.plan.descriptor.UserMapTaskDescriptor;
import com.hp.hplc.plan.descriptor.UserReduceTaskDescriptor;

public class Metastore {
	public static  int META_AVAILABLE_THRES = 1000;

	private Map<String, IndexOperatorMetadata> idxOpMetas = new HashMap<String, IndexOperatorMetadata>();
	private Map<String, IndexMetadata> idxMetas = new HashMap<String, IndexMetadata>();
	private Map<String, UserMRMetadata> mrMetas = new HashMap<String, UserMRMetadata>();
	private boolean bEmpty = true;
	

	
	public boolean isbEmpty() {
		return bEmpty;
	}

	public void setbEmpty(boolean bEmpty) {
		this.bEmpty = bEmpty;
	}

	public Metastore() {
		try {
			this.load();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void setIdxOpMeta(String idxOpName, IndexOperatorMetadata meta) {
		idxOpMetas.put(idxOpName, meta);
	}

	public void setMRMeta(String mrName, UserMRMetadata meta) {
		mrMetas.put(mrName, meta);
	}
	
	public IndexMetadata getIdxMeta(String name){
		return idxMetas.get(name);
	}

	public IndexOperatorMetadata getIdxOpMeta(String idxOpName) {
		return idxOpMetas.get(idxOpName);
	}

	public UserMRMetadata getMRMeta(String mrName) {
		return mrMetas.get(mrName);
	}

	public void load() throws ClassNotFoundException {
		Document docJDOM;
		SAXBuilder bSAX = new SAXBuilder(false);
		try {
			docJDOM = bSAX.build(new FileReader("metastore.xml"));
		}
		catch (JDOMException e) {
			e.printStackTrace();
			return;
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
		bEmpty = false;
		Element elmtRoot = docJDOM.getRootElement();
		Element idxOpChapter = elmtRoot.getChild("idxOpMetas");
		
		List<Element> idxOps = idxOpChapter.getChildren();
		Iterator<Element> itIdxOp = idxOps.iterator();
		while(itIdxOp.hasNext()){
			Element idxOp = itIdxOp.next();
			IndexOperatorMetadata idxOpMeta = IndexOperatorMetadata.buildMetadata(idxOp);
			this.idxOpMetas.put(idxOpMeta.getIdxOpName(), idxOpMeta);
		}
		
		Element idxChapter = elmtRoot.getChild("idxMetas");
		List<Element> idxes = idxChapter.getChildren();
		Iterator<Element> itIdx = idxes.iterator();
		while(itIdx.hasNext()){
			Element idx = itIdx.next();
			IndexMetadata idxMeta = IndexMetadata.buildMetadata(idx);
			this.idxMetas.put(idxMeta.getIdxName(), idxMeta);
		}
		
		Element mrChapter = elmtRoot.getChild("mrMetas");
		List<Element> mrs = mrChapter.getChildren();
		Iterator<Element> itMR = mrs.iterator();
		while(itMR.hasNext()){
			Element mr = itMR.next();
			UserMRMetadata mrMeta = UserMRMetadata.buildMetadata(mr);
			this.mrMetas.put(mrMeta.getMrName(), mrMeta);
		}

		/*IndexOperatorMetadata idxOpMeta = new IndexOperatorMetadata(ProfileIndexOperator.class.toString());
		idxOpMeta.setNumOfIndexes(2);
		idxOpMeta.setBMetaAvailalbe(false);
		this.setIdxOpMeta(ProfileIndexOperator.class.toString(), idxOpMeta);

		IndexMetadata idxMeta = new IndexMetadata("Hash://localhost/Profile");
		idxMeta.setInputKeyClass(Text.class);
		idxMeta.setOutputValueClass(Text.class);		
		this.idxMetas.put("Hash://localhost/Profile", idxMeta);
		
		IndexMetadata idxMeta1 = new IndexMetadata("Hash://localhost/Profile1");
		idxMeta1.setInputKeyClass(Text.class);
		idxMeta1.setOutputValueClass(Text.class);		
		this.idxMetas.put("Hash://localhost/Profile1", idxMeta1);*/

	}
	
	public void save() {
		Element elmtRoot = new Element("Metastore");
		Document docJDOM = new Document(elmtRoot);
		
		//this.idxOpMetas
		Element idxOpChapter = new Element("idxOpMetas");
		elmtRoot.addContent(idxOpChapter);
		
		Set<String> idxOpKeySet = this.idxOpMetas.keySet();
		Iterator<String> idxOpKeyItr = idxOpKeySet.iterator();
		while(idxOpKeyItr.hasNext()){
			String idxOpKey = idxOpKeyItr.next();
			IndexOperatorMetadata idxOpMetadata = idxOpMetas.get(idxOpKey);
			idxOpMetadata.composeXmlElement(idxOpChapter);
		}		
		
		//this.idxMetas
		Element idxChapter = new Element("idxMetas");
		elmtRoot.addContent(idxChapter);
		Set<String> idxKeySet = this.idxMetas.keySet();
		Iterator<String> idxKeyItr = idxKeySet.iterator();
		while(idxKeyItr.hasNext()){
			String idxKey = idxKeyItr.next();
			IndexMetadata idxMeta = this.idxMetas.get(idxKey);
			idxMeta.composeXmlElement(idxChapter);
		}		
		
		//this.mrMetas
		Element mrChapter = new Element("mrMetas");
		elmtRoot.addContent(mrChapter);
		Set<String> mrKeySet = this.mrMetas.keySet();
		Iterator<String> mrKeyItr = mrKeySet.iterator();
		while(mrKeyItr.hasNext()){
			String mrKey = mrKeyItr.next();
			UserMRMetadata mrMeta = this.mrMetas.get(mrKey);
			mrMeta.composeXmlElement(mrChapter);
		}		
		
		outputXML(docJDOM, "metastore.xml");
	}
	
	private void outputXML(Document docXML, String strFilename) {

		Format format = Format.getPrettyFormat();
		format.setEncoding("big5");

		XMLOutputter fmt = new XMLOutputter(format);
		try {
			FileWriter fwXML = new FileWriter(strFilename);
			fmt.output(docXML, fwXML);
			fwXML.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}

	}
	
	

	public String getInputAndIndexMappingStr(JobConf job, __IndexAccessor accessor) throws Exception{
		List<String>[] lists = accessor.getPartitionLocations();
		InputAndIndexMapping inim = new InputAndIndexMapping();
		int numOfPart = lists.length;
		InputFormat inputFormat = job.getInputFormat();
		if(inputFormat instanceof FileInputFormat){
			FileInputFormat fif = (FileInputFormat)inputFormat;
			FileStatus[] fss = fif.getFiles(job);
			if(fss.length > numOfPart){
				throw new Exception("Partition doesn't match.");
			}
			
			for(FileStatus fs: fss){
				String fileName = fs.getPath().toString();
				String[] strs = fileName.split("/");
				String partitionName = strs[strs.length-1].split("\\.")[0];
				System.out.println("fileName = " + fileName);
				System.out.println("partitionName = " + partitionName);
				int partitionId = Integer.parseInt(partitionName.substring(6));
				if(partitionId >= numOfPart){
					throw new Exception("Non-existed partition : " + partitionName);
				}
				List<String> list = lists[partitionId];
				Iterator<String> it = list.iterator();
				String nodesStr = "";
				while(it.hasNext()){
					String str = it.next();
					nodesStr += str + ",";
				}
				inim.addMapping(Integer.toString(partitionId), nodesStr);
			}
		}		
		
		
		/*int counter = 0;
		for(List<String> list: lists){
			Iterator<String> it = list.iterator();
			String nodesStr = "";
			while(it.hasNext()){
				String str = it.next();
				nodesStr += str + ",";
			}
			inim.addMapping(""+counter, nodesStr);
		}*/
		return inim.pack();
	}
	
	public String getInputAndIndexMappingStr1(JobConf job, __IndexAccessor accessor) throws Exception{
		
		InputAndIndexMapping inim = new InputAndIndexMapping();
		InputFormat inputFormat = job.getInputFormat();
		if(inputFormat instanceof FileInputFormat){
			FileInputFormat fif = (FileInputFormat)inputFormat;
			FileStatus[] fss = fif.getFiles(job);
			for(FileStatus fs: fss){
				String fileName = fs.getPath().toString();
				String[] strs = fileName.split("/");
				String partitionName = strs[strs.length-1].split("\\.")[0];
				System.out.println("fileName = " + fileName);
				System.out.println("partitionName = " + partitionName);
				inim.addMapping(fileName, getPartitionMapping(partitionName));
			}
		}		
		return inim.pack();
	}
	
	public String getPartitionMapping(String partitionName){
		//actually, we need to get this information from Cassandra
		return "hplcchina-HP-xw8600-Workstation";
	}

	/*public String getInputAndIndexMappingStr() {
		InputAndIndexMapping inim = new InputAndIndexMapping();

		inim.addMapping("hdfs://localhost:9000/user/hplcchina/input/1.txt", "hplcchina-HP-xw8600-Workstation");
		inim.addMapping("hdfs://localhost:9000/user/hplcchina/input/2.txt", "hplcchina-HP-xw8600-Workstation");
		inim.addMapping("hdfs://localhost:9000/user/hplcchina/input/3.txt", "hplcchina-HP-xw8600-Workstation");
		inim.addMapping("hdfs://localhost:9000/user/hplcchina/input/4.txt", "hplcchina-HP-xw8600-Workstation");
		inim.addMapping("hdfs://localhost:9000/user/hplcchina/input/5.txt", "hplcchina-HP-xw8600-Workstation");
		inim.addMapping("hdfs://localhost:9000/user/hplcchina/EmptyInput/Empty", "hplcchina-HP-xw8600-Workstation");*/

	/*	inim.addMapping("file:/home/hplcchina/workspace/indexoperator-project/input/1.txt", "hplcchina-HP-xw8600-Workstation");
		inim.addMapping("file:/home/hplcchina/workspace/indexoperator-project/input/2.txt", "hplcchina-HP-xw8600-Workstation");
		inim.addMapping("file:/home/hplcchina/workspace/indexoperator-project/input/3.txt", "hplcchina-HP-xw8600-Workstation");
		inim.addMapping("file:/home/hplcchina/workspace/indexoperator-project/input/4.txt", "hplcchina-HP-xw8600-Workstation");
		inim.addMapping("file:/home/hplcchina/workspace/indexoperator-project/input/5.txt", "hplcchina-HP-xw8600-Workstation");
		inim.addMapping("file:/home/hplcchina/workspace/indexoperator-project/EmptyInput/empty", "hplcchina-HP-xw8600-Workstation");
		*/
		/*return inim.pack();
	}*/

	public String getPartitionAndIndexMappingStr(__IndexAccessor accessor) {
		
		PartitionAndIndexMapping paim = new PartitionAndIndexMapping();
		List<String>[] lists = accessor.getPartitionLocations();
		int counter = 0;
		for(List<String> list: lists){
			Iterator<String> it = list.iterator();
			String nodesStr = "";
			while(it.hasNext()){
				String str = it.next();
				nodesStr += str + ",";
			}
			
			paim.addMapping(counter, nodesStr);
			counter++;
		}

		//paim.addMapping(0, "hplcchina-HP-xw8600-Workstation");
		//paim.addMapping(1, "hplcchina-HP-xw8600-Workstation");
		//paim.addMapping(2, "hplcchina-HP-xw8600-Workstation");

		return paim.pack();
	}

	public void update(IndexJobConf indexJobConf, org.apache.hadoop.mapreduce.Counters counters, Plan plan, int jId) {
		
		
	/*	Vector<TaskDescriptor> tds = plan.getJob(jId);
		Iterator<TaskDescriptor> tdItr = tds.iterator();
		while(tdItr.hasNext()){
			TaskDescriptor td = tdItr.next();
			if(td instanceof IndexPreTaskDescriptor){
				
				try {
					__IndexOperator idxOp = ((IndexPreTaskDescriptor) td).getTask();
					updateIdxPreMeta(idxOp, counters, td.getID());
				} catch (InstantiationException e) {
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				}				
			}
			
			if(td instanceof IndexLookupTaskDescriptor){
				__IndexOperator idxOp = ((IndexLookupTaskDescriptor) td).getTask();
				updateIdxLookupMeta(idxOp, counters, td.getID());
			}
			
			if(td instanceof IndexPostTaskDescriptor){
				__IndexOperator idxOp = ((IndexPostTaskDescriptor) td).getTask();
				updateIdxPostMeta(idxOp, counters, td.getID());
			}
			
			if(td instanceof UserMapTaskDescriptor){
				Class<? extends Mapper<Writable, Writable, Writable, Writable> > task = ((UserMapTaskDescriptor) td).getTask();
				updateUserMRMeta(task.getName(), counters, td.getID());
			}
			
			if(td instanceof UserReduceTaskDescriptor){
				Class<? extends Reducer<Writable, Writable, Writable, Writable> > task = ((UserReduceTaskDescriptor) td).getTask();
				updateUserMRMeta(task.getName(), counters, td.getID());
			}
		}*/

 		int taskId = 0;
		// IndexOperators before User Map
		java.util.Iterator itHead = indexJobConf.getHeadIndexOperators().iterator();
		while (itHead.hasNext()) {
			Object obj = itHead.next();			
			__IndexOperator idxOp = (__IndexOperator) obj;
			
			updateIdxPreMeta(idxOp, counters, taskId);
			taskId++;
			updateIdxLookupMeta(idxOp, counters, taskId);
			taskId++;
			updateIdxPostMeta(idxOp, counters, taskId);
			taskId++;
		}

		// user Map
		if (indexJobConf.getMapperClass() != null) {
			taskId++;
			updateUserMRMeta(indexJobConf.getMapperClass().getName(), counters, taskId);
		}

		// Index Operators between user map and user reduce
		java.util.Iterator itBody = indexJobConf.getBodyIndexOperators().iterator();
		while (itBody.hasNext()) {
			Object obj = itBody.next();			
			__IndexOperator idxOp = (__IndexOperator) obj;
			
			updateIdxPreMeta(idxOp, counters, taskId);
			taskId++;
			updateIdxLookupMeta(idxOp, counters, taskId);
			taskId++;
			updateIdxPostMeta(idxOp, counters, taskId);
			taskId++;
		}

		//User reduce
		if (indexJobConf.getReducerClass() != null) {
			taskId++;
			updateUserMRMeta(indexJobConf.getMapperClass().getName(), counters, taskId);
		}

		// Index operators after user reduce
		java.util.Iterator itTail = indexJobConf.getTailIndexOperators().iterator();
		while (itTail.hasNext()) {
			Object obj = itTail.next();
			__IndexOperator idxOp = (__IndexOperator) obj;
			
			updateIdxPreMeta(idxOp, counters, taskId);
			taskId++;
			updateIdxLookupMeta(idxOp, counters, taskId);
			taskId++;
			updateIdxPostMeta(idxOp, counters, taskId);
			taskId++;
		}
	}

	private void updateUserMRMeta(String name, Counters counters, int taskId) {
		UserMRMetadata userMRMeta = this.mrMetas.get(name);
		
		Counter numInputRecordCounter = null;
		Counter inputKeyBytesCounter = null;
		Counter inputValueBytesCounter = null;
		Counter numOutputRecordCounter = null;
		Counter outputKeyBytesCounter = null;
		Counter outputValueBytesCounter = null;
		
		long numInputRecord = 0;
		long inputKeyBytes = 0;
		long inputValueBytes = 0;
		long numOutputRecord = 0;
		long outputKeyBytes = 0;
		long outputValueBytes = 0;
		
		// input
		numInputRecordCounter = counters.findCounter(IndexCounter.GROUP,
				IndexCounter.get(taskId, IndexCounter.TASK_INPUT_RECORDS));
		if (numInputRecordCounter != null) {
			numInputRecord = numInputRecordCounter.getValue();
		}

		inputKeyBytesCounter = counters.findCounter(IndexCounter.GROUP,
				IndexCounter.get(taskId, IndexCounter.TASK_INPUT_KEY_BYTES));
		inputKeyBytes = inputKeyBytesCounter.getValue();

		inputValueBytesCounter = counters.findCounter(IndexCounter.GROUP,
				IndexCounter.get(taskId, IndexCounter.TASK_INPUT_VALUE_BYTES));
		inputValueBytes = inputValueBytesCounter.getValue();

		// output
		numOutputRecordCounter = counters.findCounter(IndexCounter.GROUP,
				IndexCounter.get(taskId, IndexCounter.TASK_OUTPUT_RECORDS));
		if (numOutputRecordCounter != null) {
			numOutputRecord = numOutputRecordCounter.getValue();
		}

		outputKeyBytesCounter = counters.findCounter(IndexCounter.GROUP,
				IndexCounter.get(taskId, IndexCounter.TASK_OUTPUT_KEY_BYTES));
		outputKeyBytes = outputKeyBytesCounter.getValue();

		outputValueBytesCounter = counters.findCounter(IndexCounter.GROUP,
				IndexCounter.get(taskId, IndexCounter.TASK_OUTPUT_VALUE_BYTES));
		outputValueBytes = outputValueBytesCounter.getValue();
		
		if(numInputRecord != 0){
			userMRMeta.setProductivity((double)numOutputRecord / numInputRecord);
		}
		
		if(numOutputRecord != 0){
			userMRMeta.setAvgSize((int)((outputKeyBytes + outputValueBytes)/numOutputRecord));
		}
				
	}

	private void updateIdxPreMeta(__IndexOperator idxOp, org.apache.hadoop.mapreduce.Counters counters, int taskId) {
		IndexOperatorMetadata idxOpMeta = idxOpMetas.get(idxOp.getClass().toString());
		
		Counter numInputRecordCounter = null;
		Counter inputKeyBytesCounter = null;
		Counter inputValueBytesCounter = null;
		Counter numOutputRecordCounter = null;
		Counter outputKeyBytesCounter = null;
		Counter outputValueBytesCounter = null;
		
		long numInputRecord = 0;
		long inputKeyBytes = 0;
		long inputValueBytes = 0;
		long numOutputRecord = 0;
		long outputKeyBytes = 0;
		long outputValueBytes = 0;
		
		//input
		numInputRecordCounter = counters.findCounter(IndexCounter.GROUP,
				IndexCounter.get(taskId, IndexCounter.TASK_INPUT_RECORDS));		
		if(numInputRecordCounter != null){
			numInputRecord = numInputRecordCounter.getValue();
		}
		
		if(idxOp.getInputKeyClass()!= Text.class && idxOp.getInputKeyClass() !=BytesWritable.class){
			inputKeyBytes = IndexCounter.getBytes((int) numInputRecord, 0, idxOp.getInputKeyClass());
		}else{
			inputKeyBytesCounter = counters.findCounter(IndexCounter.GROUP,
					IndexCounter.get(taskId, IndexCounter.TASK_INPUT_KEY_BYTES));	
			inputKeyBytes = inputKeyBytesCounter.getValue();
		}
		
		if(idxOp.getInputValueClass()!= Text.class && idxOp.getInputValueClass() !=BytesWritable.class){
			inputValueBytes = IndexCounter.getBytes((int) numInputRecord, 0, idxOp.getInputValueClass());
		}else{
			inputValueBytesCounter = counters.findCounter(IndexCounter.GROUP,
					IndexCounter.get(taskId, IndexCounter.TASK_INPUT_VALUE_BYTES));	
			inputValueBytes = inputValueBytesCounter.getValue();
		}
		
		//output
		numOutputRecordCounter = counters.findCounter(IndexCounter.GROUP,
				IndexCounter.get(taskId, IndexCounter.TASK_OUTPUT_RECORDS));	
		if(numOutputRecordCounter != null){
			numOutputRecord = numOutputRecordCounter.getValue();
		}
		
		if(idxOp.getPreProKeyClass() !=Text.class && idxOp.getPreProKeyClass() != BytesWritable.class){
			outputKeyBytes = IndexCounter.getBytes((int) numOutputRecord, 0, idxOp.getPreProKeyClass());
		}else{
			outputKeyBytesCounter = counters.findCounter(IndexCounter.GROUP,
					IndexCounter.get(taskId, IndexCounter.TASK_OUTPUT_KEY_BYTES));		
			outputKeyBytes = outputKeyBytesCounter.getValue();
		}
		
		if (idxOp.getPreProValueClass() != Text.class && idxOp.getPreProKeyClass() != BytesWritable.class) {
			outputValueBytes = IndexCounter.getBytes((int) numOutputRecord, 0, idxOp.getPreProValueClass());
		} else {
			outputValueBytesCounter = counters.findCounter(IndexCounter.GROUP,
					IndexCounter.get(taskId, IndexCounter.TASK_OUTPUT_VALUE_BYTES));
			outputValueBytes = outputValueBytesCounter.getValue();
		}
		
		//print
		/*System.out.println(numInputRecordCounter.getName() + " = " + numInputRecordCounter.getValue());
		System.out.println(inputKeyBytesCounter.getName() + " = " + inputKeyBytesCounter.getValue());
		System.out.println(inputValueBytesCounter.getName() + " = " + inputValueBytesCounter.getValue());
		System.out.println(numOutputRecordCounter.getName() + " = " + numOutputRecordCounter.getValue());
		System.out.println(outputKeyBytesCounter.getName() + " = " + outputKeyBytesCounter.getValue());
		System.out.println(outputValueBytesCounter.getName() + " = " + outputValueBytesCounter.getValue());*/
		
		//update
		idxOpMeta.setNumOfInputRecord(numInputRecord);
		
		if (numOutputRecord != 0.0 && numInputRecord != 0.0) {
			double selectivity = ((double) numOutputRecord) / numInputRecord;
			idxOpMeta.setSeleOfPrePro(selectivity);
		}
		if (numInputRecord != 0) {
			int sizeOfInputRecord = (int) ((inputKeyBytes + inputValueBytes) / numInputRecord);
			idxOpMeta.setSizeOfInputRecord(sizeOfInputRecord);
		}
		if (numOutputRecord != 0) {
			int svapp = (int) ((outputKeyBytes + outputValueBytes) / numOutputRecord);
			idxOpMeta.setSizeOfValueAfterPrePro(svapp);
		}
		
		if(numInputRecord > META_AVAILABLE_THRES){
			idxOpMeta.setBMetaAvailalbe(true);
		}
	}
	
	private void updateIdxLookupMeta(__IndexOperator idxOp, Counters counters, int taskId) {
		IndexOperatorMetadata idxOpMeta = idxOpMetas.get(idxOp.getClass().toString());
		
		int numIndexes = idxOp.size();
		
		long nir = 0;
		Counter numOutputRecordCounter = counters.findCounter(IndexCounter.GROUP,
				IndexCounter.get(taskId-1, IndexCounter.TASK_OUTPUT_RECORDS));	
		if(numOutputRecordCounter != null){
			nir = numOutputRecordCounter.getValue();
		}
		

		for (int index = 0; index < numIndexes; index++) {
			Counter indexInputKeysCounter = null;
			Counter indexInputBytesCounter = null;
			Counter indexOutputValuesCounter = null;
			Counter indexOutputBytesCounter = null;
			Counter indexCacheHitCounter = null;
			Counter indexCacheMissCounter = null;
			Counter indexLookupTimeCounter = null;
			
			long numIndexInputKeys = 0;
			long indexInputBytes = 0;
			long numIndexOutputValues = 0;
			long indexOutputBytes = 0;
			long indexCacheHit = 0;
			long indexCacheMiss = 0;
			double indexCacheHitRate = 0.0;
			double avgIndexLookupTime = 0;
			
			indexInputKeysCounter = counters.findCounter(IndexCounter.GROUP,
					IndexCounter.get(taskId, index, IndexCounter.INDEX_INPUT_KEYS));
			if(indexInputKeysCounter != null){
				numIndexInputKeys = indexInputKeysCounter.getValue();
			}

			String idxNameStr = idxOp.getAccessStr(index);
			Class<? extends Writable> indexInputClass = idxMetas.get(idxNameStr).getInputKeyClass();
			if (indexInputClass != Text.class && indexInputClass != BytesWritable.class) {
				indexInputBytes = IndexCounter.getBytes((int) numIndexInputKeys, 0, indexInputClass);
			} else {
				indexInputBytesCounter = counters.findCounter(IndexCounter.GROUP,
						IndexCounter.get(taskId, index, IndexCounter.INDEX_INPUT_BYTES));
				indexInputBytes = indexInputBytesCounter.getValue();
			}
			
			indexOutputValuesCounter = counters.findCounter(IndexCounter.GROUP,
					IndexCounter.get(taskId, index, IndexCounter.INDEX_OUTPUT_VALUES));
			if(indexOutputValuesCounter != null){
				numIndexOutputValues = indexOutputValuesCounter.getValue();
			}

			Class<? extends Writable> indexOutputClass = idxMetas.get(idxNameStr).getOutputValueClass();
			if (indexOutputClass != Text.class && indexOutputClass != BytesWritable.class) {
				indexOutputBytes = IndexCounter.getBytes((int) numIndexOutputValues, 0, indexOutputClass);
			} else {
				indexOutputBytesCounter = counters.findCounter(IndexCounter.GROUP,
						IndexCounter.get(taskId, index, IndexCounter.INDEX_OUTPUT_BYTES));
				indexOutputBytes = indexOutputBytesCounter.getValue();
			}			

			indexCacheHitCounter = counters.findCounter(IndexCounter.GROUP,
					IndexCounter.get(taskId, index, IndexCounter.INDEX_CACHE_HIT));
			indexCacheMissCounter = counters.findCounter(IndexCounter.GROUP,
					IndexCounter.get(taskId, index, IndexCounter.INDEX_CACHE_MISS));
			
			if(indexCacheHitCounter != null){
				indexCacheHit = indexCacheHitCounter.getValue();
			}
			
			if(indexCacheMissCounter != null){				
				indexCacheMiss = indexCacheMissCounter.getValue();
			}
			
			//System.out.println("\tIndexCacheHit = " + indexCacheHit);
			//System.out.println("\tIndexCacheMiss = " + indexCacheMiss);
			
			if((indexCacheHit + indexCacheMiss)!=0){
				indexCacheHitRate = ((double)indexCacheHit)/(indexCacheHit + indexCacheMiss);
			}
			
			indexLookupTimeCounter = counters.findCounter(IndexCounter.GROUP,
					IndexCounter.get(taskId, index, IndexCounter.INDEX_LOOKUP_TIME));
			//System.out.println("indexLookupTimeCounter = " + indexLookupTimeCounter);
			//System.out.println("indexCacheMiss = " + indexCacheMiss);
			if(indexLookupTimeCounter != null){
				if(indexCacheMiss != 0){
					//System.out.println("indexLookupTimeCounter.getValue() = " + indexLookupTimeCounter.getValue());
					avgIndexLookupTime = ((double)indexLookupTimeCounter.getValue())/indexCacheMiss;
					//System.out.println("avgIndexLookupTime = " + avgIndexLookupTime);
				}
			}

			/*System.out.println("Index " + index + " " + indexInputKeysCounter.getName() + " = "
					+ indexInputKeysCounter.getValue());
			System.out.println("Index " + index + " " + indexInputBytesCounter.getName() + " = "
					+ indexInputBytesCounter.getValue());
			System.out.println("Index " + index + " " + indexOutputValuesCounter.getName() + " = "
					+ indexOutputValuesCounter.getValue());
			System.out.println("Index " + index + " " + indexOutputBytesCounter.getName() + " = "
					+ indexOutputBytesCounter.getValue());
			System.out.println("Index " + index + " " + indexCacheHitCounter.getName() + " = "
					+ indexCacheHitCounter.getValue());
			System.out.println("Index " + index + " " + indexCacheMissCounter.getName() + " = "
					+ indexCacheMissCounter.getValue());*/
			int nikpr = 1;
			if (nir != 0) {
				//System.out.println("\tnumIndexInputKeys = " + numIndexInputKeys);
				//System.out.println("\tnir = " + nir);
				nikpr = (int) (numIndexInputKeys / nir);
				if(nikpr < 1)
					nikpr = 1;
			}
			//System.out.println("\tnikpr = " + nikpr);
			idxOpMeta.setNumOfIndexKeyPerRecord(index, nikpr);
			
			if(numIndexInputKeys != 0){

				int sik = (int) (indexInputBytes / numIndexInputKeys);
				idxOpMeta.setSizeOfIndexKey(index, sik);

				int siv = (int) (indexOutputBytes / numIndexInputKeys);
				idxOpMeta.setSizeOfIndexValue(index, siv);
			}
			if(avgIndexLookupTime != 0){
				idxOpMeta.setAvgIndexLookupTime(index, avgIndexLookupTime);
			}
			
			if(indexCacheHitRate != 0.0){
				idxOpMeta.setCacheHitRate(index, indexCacheHitRate);
			}
		}
	}
	
	private void updateIdxPostMeta(__IndexOperator idxOp, Counters counters, int taskId){
		IndexOperatorMetadata idxOpMeta = this.getIdxOpMeta(idxOp.getClass().toString());
		
		Counter numOutputRecordCounter = counters.findCounter(IndexCounter.GROUP,
				IndexCounter.get(taskId, IndexCounter.TASK_OUTPUT_RECORDS));
		
		Counter outputKeyBytesCounter = counters.findCounter(IndexCounter.GROUP,
				IndexCounter.get(taskId, IndexCounter.TASK_OUTPUT_KEY_BYTES));
		
		Counter outputValueBytesCounter = counters.findCounter(IndexCounter.GROUP,
				IndexCounter.get(taskId, IndexCounter.TASK_OUTPUT_VALUE_BYTES));
		
		long numInputRecord = idxOpMeta.getNumOfInputRecord();
		
		long numOutputRecord = 0;
		if(numOutputRecordCounter != null){
			numOutputRecord = numOutputRecordCounter.getValue();
		}
		
		long outputKeyBytes = 0;
		if(outputKeyBytesCounter != null){
			outputKeyBytes = outputKeyBytesCounter.getValue();
		}
		
		long outputValueBytes = 0;
		if(outputValueBytesCounter != null){
			outputValueBytes = outputValueBytesCounter.getValue();
		}
		
		if(numInputRecord != 0){
			idxOpMeta.setPostProductivity((double)numOutputRecord/((double)numInputRecord*idxOpMeta.getSeleOfPrePro()));
		}
		
		if(numOutputRecord != 0){
			idxOpMeta.setSizeOfValueAfterPostPro((int)((outputKeyBytes + outputValueBytes)/numOutputRecord));
		}
		
		/*System.out.println(numOutputRecordCounter.getName() + " = " + numOutputRecordCounter.getValue());
		System.out.println(outputKeyBytesCounter.getName() + " = " + outputKeyBytesCounter.getValue());
		System.out.println(outputValueBytesCounter.getName() + " = " + outputValueBytesCounter.getValue());*/
		
	}
	
	public void print(){
		Set<String> idxOpKeySet = this.idxOpMetas.keySet();
		Iterator<String> idxOpKeyItr = idxOpKeySet.iterator();
		while(idxOpKeyItr.hasNext()){
			String idxOpKey = idxOpKeyItr.next();
			IndexOperatorMetadata idxOpMetadata = idxOpMetas.get(idxOpKey);
			idxOpMetadata.print();
		}
		
		Set<String> idxKeySet = this.idxMetas.keySet();
		Iterator<String> idxKeyItr = idxKeySet.iterator();
		while(idxKeyItr.hasNext()){
			String idxKey = idxKeyItr.next();
			IndexMetadata idxMeta = this.idxMetas.get(idxKey);
			idxMeta.print();
		}		
	}

	public void initIdxOpMeta(__IndexOperator idxOp) {
		String idxOpName = idxOp.getClass().toString();
		IndexOperatorMetadata idxOpMeta = this.idxOpMetas.get(idxOpName);
		
		if (idxOpMeta != null && idxOpMeta.getNumOfIndexes() == idxOp.size()) {
		
		} else {
			if (idxOpMeta != null) {
				idxOpMetas.remove(idxOpName);
			}

			idxOpMeta = new IndexOperatorMetadata(idxOpName);
			idxOpMeta.setNumOfIndexes(idxOp.size());
			idxOpMeta.setBMetaAvailalbe(false);
			idxOpMetas.put(idxOpName, idxOpMeta);
		}		

		int numIndexes = idxOp.size();
		for(int index = 0; index<numIndexes; index++){
			String name = idxOp.getNames().get(index);
			String url = idxOp.getUrls().get(index);
			IndexMetadata idxMeta = this.idxMetas.get(url);
			if(idxMeta != null){
				
			}else{
				idxMeta = new IndexMetadata(url);
				__IndexAccessor accessor = idxOp.getIndexAccessor(index);
				idxMeta.setInputKeyClass(accessor.getKeyClass());
				idxMeta.setOutputValueClass(accessor.getValueClass());
				this.idxMetas.put(url, idxMeta);
			}
		}
	}

	public void initUserMRMeta(String  mrName) {
		UserMRMetadata userMRMeta= this.mrMetas.get(mrName);
		if(userMRMeta != null){
			
		}else{
			userMRMeta = new UserMRMetadata(mrName, 0, 0);
			this.mrMetas.put(mrName, userMRMeta);
		}
		
	}

}
