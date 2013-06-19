/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce.split;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.mapreduce.split.JobSplit.SplitMetaInfo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The class that is used by the Job clients to write splits (both the meta
 * and the raw bytes parts)
 */
public class JobSplitWriter {

  private static final Log LOG = LogFactory.getLog(JobSplitWriter.class);
  private static final int splitVersion = JobSplit.META_SPLIT_VERSION;
  private static final byte[] SPLIT_FILE_HEADER;
  static final String MAX_SPLIT_LOCATIONS = "mapreduce.job.max.split.locations";
  public static final String B_RUN_MAP_AT_INDEX = "filesplit.index.brunatindex";
  public static final String RUN_MAP_AT_INDEX_ID = "filesplit.index.runatindexid";
  public static final String INPUT_AND_INDEX_MAPPING = "filesplit.index.inputandindexmapping";
  
  static {
    try {
      SPLIT_FILE_HEADER = "SPL".getBytes("UTF-8");
    } catch (UnsupportedEncodingException u) {
      throw new RuntimeException(u);
    }
  }
  
  @SuppressWarnings("unchecked")
  public static <T extends InputSplit> void createSplitFiles(Path jobSubmitDir, 
      Configuration conf, FileSystem fs, List<InputSplit> splits) 
  throws IOException, InterruptedException {
    T[] array = (T[]) splits.toArray(new InputSplit[splits.size()]);
    createSplitFiles(jobSubmitDir, conf, fs, array);
  }
  
  public static <T extends InputSplit> void createSplitFiles(Path jobSubmitDir, 
      Configuration conf, FileSystem fs, T[] splits) 
  throws IOException, InterruptedException {
    FSDataOutputStream out = createFile(fs, 
        JobSubmissionFiles.getJobSplitFile(jobSubmitDir), conf);
    SplitMetaInfo[] info = writeNewSplits(conf, splits, out);
    out.close();
    writeJobSplitMetaInfo(fs,JobSubmissionFiles.getJobSplitMetaFile(jobSubmitDir), 
        new FsPermission(JobSubmissionFiles.JOB_FILE_PERMISSION), splitVersion,
        info);
  }
  
  public static void createSplitFiles(Path jobSubmitDir, 
      Configuration conf, FileSystem   fs, 
      org.apache.hadoop.mapred.InputSplit[] splits) 
  throws IOException {
    FSDataOutputStream out = createFile(fs, 
        JobSubmissionFiles.getJobSplitFile(jobSubmitDir), conf);
    SplitMetaInfo[] info = writeOldSplits(splits, out, conf);
    out.close();
    writeJobSplitMetaInfo(fs,JobSubmissionFiles.getJobSplitMetaFile(jobSubmitDir), 
        new FsPermission(JobSubmissionFiles.JOB_FILE_PERMISSION), splitVersion,
        info);
  }
  
  private static FSDataOutputStream createFile(FileSystem fs, Path splitFile, 
      Configuration job)  throws IOException {
    FSDataOutputStream out = FileSystem.create(fs, splitFile, 
        new FsPermission(JobSubmissionFiles.JOB_FILE_PERMISSION));
    int replication = job.getInt("mapred.submit.replication", 10);
    fs.setReplication(splitFile, (short)replication);
    writeSplitHeader(out);
    return out;
  }
  private static void writeSplitHeader(FSDataOutputStream out) 
  throws IOException {
    out.write(SPLIT_FILE_HEADER);
    out.writeInt(splitVersion);
  }
  
  @SuppressWarnings("unchecked")
  private static <T extends InputSplit> 
  SplitMetaInfo[] writeNewSplits(Configuration conf, 
      T[] array, FSDataOutputStream out)
  throws IOException, InterruptedException {

    SplitMetaInfo[] info = new SplitMetaInfo[array.length];
    if (array.length != 0) {
      SerializationFactory factory = new SerializationFactory(conf);
      int i = 0;
      long offset = out.size();
      for(T split: array) {
        int prevCount = out.size();
        Text.writeString(out, split.getClass().getName());
        Serializer<T> serializer = 
          factory.getSerializer((Class<T>) split.getClass());
        serializer.open(out);
        serializer.serialize(split);
        int currCount = out.size();
        String[] locations = split.getLocations();
        final int max_loc = conf.getInt(MAX_SPLIT_LOCATIONS, 10);
        if (locations.length > max_loc) {
          LOG.warn("Max block location exceeded for split: "
              + split + " splitsize: " + locations.length +
              " maxsize: " + max_loc);
          locations = Arrays.copyOf(locations, max_loc);
        }
        
        if(split instanceof FileSplit){
        	FileSplit fileSplit = (FileSplit) split;
        	String filePathStr = fileSplit.getPath().toString();
        	System.out.println("FileSplit url: " + filePathStr);
        }
        
        info[i++] = 
          new JobSplit.SplitMetaInfo( 
              locations, offset,
              split.getLength());
        offset += currCount - prevCount;
      }
    }
    return info;
  }
  
  private static HashMap<String, List<String>> buildInputAndIndexMapping(Configuration conf){
	  HashMap<String, List<String>> map = new HashMap<String, List<String>>();

	  String mappingStr = conf.get(INPUT_AND_INDEX_MAPPING);
	  //System.out.println("Mapping String:");
	  //System.out.println(mappingStr);
	  if(mappingStr != null){
		String[] lines = mappingStr.split("\n");
		for(int i=0; i<lines.length; i++){
			String[] strs = lines[i].split("#");
			//System.out.println("# of nodes: " + strs.length);
			if(strs.length>1){
				String inputFileName = strs[0];
				String[] values = strs[1].split(",");
				List<String> list = new ArrayList<String>();
				for(int j = 0; j < values.length; j++){
					list.add(values[j]);
				}
				map.put(inputFileName, list);
			}			
		}
	  }else{
		  System.out.println("Warning: no input and index mapping defined!");
	  }
	  
	  return map;
  }
  
  private static SplitMetaInfo[] writeOldSplits(
      org.apache.hadoop.mapred.InputSplit[] splits,
      FSDataOutputStream out, Configuration conf) throws IOException {
	  
	  HashMap<String, List<String>> map = buildInputAndIndexMapping(conf);
	  
    SplitMetaInfo[] info = new SplitMetaInfo[splits.length];
    if (splits.length != 0) {
      int i = 0;
      long offset = out.size();
      for(org.apache.hadoop.mapred.InputSplit split: splits) {
        int prevLen = out.size();
        Text.writeString(out, split.getClass().getName());
        split.write(out);
        int currLen = out.size();
        String[] locations = split.getLocations();
        final int max_loc = conf.getInt(MAX_SPLIT_LOCATIONS, 10);
        if (locations.length > max_loc) {
          LOG.warn("Max block location exceeded for split: "
              + split + " splitsize: " + locations.length +
              " maxsize: " + max_loc);
          locations = Arrays.copyOf(locations, max_loc);
        }
        
        if(split instanceof FileSplit){
        	FileSplit fileSplit = (FileSplit) split;
        	String filePathStr = fileSplit.getPath().toString();
        	System.out.println("\tFileSplit url: " + filePathStr);
        
			String[] strs = filePathStr.split("/");
			String partitionName = strs[strs.length-1].split("\\.")[0];
			System.out.println("\tfileName = " + filePathStr);
			System.out.println("\tpartitionName = " + partitionName);
			String partitionIdStr = (new Integer(partitionName.substring(6))).toString();
			System.out.println("\tpartitionIdStr = " + partitionIdStr);
        	
        	boolean bRunAtIndex = false;
					int indexId = -1;
					String[] indexLocations = null;
					String bRunAtIndexStr = conf.get(B_RUN_MAP_AT_INDEX);
					if (bRunAtIndexStr != null)
						bRunAtIndex = true;
					if (bRunAtIndex) {
						String indexIdStr = conf.get(RUN_MAP_AT_INDEX_ID);

						if (indexIdStr != null) {
							indexId = Integer.parseInt(indexIdStr);
						}

						List<String> tempList = map.get(partitionIdStr);
						//System.out.println(tempList.toArray());

						indexLocations = new String[tempList.size()];
						tempList.toArray(indexLocations);
					}
        	
			info[i++] = new JobSplit.SplitMetaInfo(locations, offset, split.getLength(), bRunAtIndex, indexId,
							indexLocations);
			offset += currLen - prevLen;

        }else{
        
        info[i++] = new JobSplit.SplitMetaInfo( 
            locations, offset,
            split.getLength());
        offset += currLen - prevLen;
        }
      }
    }
    return info;
  }

  private static void writeJobSplitMetaInfo(FileSystem fs, Path filename, 
      FsPermission p, int splitMetaInfoVersion, 
      JobSplit.SplitMetaInfo[] allSplitMetaInfo) 
  throws IOException {
    // write the splits meta-info to a file for the job tracker
    FSDataOutputStream out = 
      FileSystem.create(fs, filename, p);
    out.write(JobSplit.META_SPLIT_FILE_HEADER);
    WritableUtils.writeVInt(out, splitMetaInfoVersion);
    WritableUtils.writeVInt(out, allSplitMetaInfo.length);
    for (JobSplit.SplitMetaInfo splitMetaInfo : allSplitMetaInfo) {
      splitMetaInfo.write(out);
    }
    out.close();
  }
}

