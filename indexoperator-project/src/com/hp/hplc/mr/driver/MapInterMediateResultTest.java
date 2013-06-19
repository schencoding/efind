package com.hp.hplc.mr.driver;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;

import javax.crypto.SecretKey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.security.SecureShuffleUtils;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.security.token.Token;

public class MapInterMediateResultTest {
	
	private URL url;
	private String jobId;
	

	public MapInterMediateResultTest(URL url, String jobId) {
		super();
		this.url = url;
		this.jobId = jobId;
	}

	public void print() throws IOException {
		Token<JobTokenIdentifier> jt = new Token<JobTokenIdentifier>();

		Configuration hdfsConf = new Configuration();
		FileSystem hdfs = FileSystem.get(hdfsConf);
		Path hdfsFile = new Path("hdfs://localhost:9000/user/hplcchina/" + jobId+"_token");
		FSDataInputStream in = hdfs.open(hdfsFile);
		jt.readFields(in);
		in.close();
	//	URL url = new URL(
	//			"http://localhost:50060/mapOutput?job=job_201205090150_0001&map=attempt_201205090150_0001_m_000000_0&reduce=0");

		URLConnection connection;
		try {
			connection = url.openConnection();
			connection.setReadTimeout(2000000);
			connection.setConnectTimeout(200000);

			SecretKey secretKey = JobTokenSecretManager.createSecretKey(jt.getPassword());

			String msgToEncode = SecureShuffleUtils.buildMsgFrom(connection.getURL());
			String encHash = SecureShuffleUtils.hashFromString(msgToEncode, secretKey);

			// put url hash into http header
			connection.setRequestProperty(SecureShuffleUtils.HTTP_HEADER_URL_HASH, encHash);

			System.out.println("doInput = " + connection.getDoInput());
			System.out.println("doOutput = " + connection.getDoOutput());
			// connection.setDoOutput(true);

			connection.connect();

			// BufferedReader input = new BufferedReader(new
			// InputStreamReader(connection.getInputStream()));

			String replyHash = connection.getHeaderField(SecureShuffleUtils.HTTP_HEADER_REPLY_URL_HASH);
			System.out.println("replyHash: " + replyHash);
			if (replyHash == null) {
				throw new IOException("security validation of TT Map output failed");
			}

			SecureShuffleUtils.verifyReply(replyHash, encHash, secretKey);
			InputStream is = connection.getInputStream();
			BufferedReader input = new BufferedReader(new
					InputStreamReader(is));
			String str = null;
			while((str = input.readLine()) != null)
				System.out.println(str);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/*public static void main(String[] args) throws IOException {
		MapInterMediateResultTest test = new MapInterMediateResultTest();
		test.print();
	}*/

}
