/*
 * Estado: Muti cluster Hadoop job status metric collector
 * Author: Pranab Ghosh
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */


package org.estado.spi;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.sql.Timestamp;

public class JobStatusRdbmsConsumer implements JobStatusConsumer {

	private String url;
	private Connection connect;
	private PreparedStatement crPrepStmt;
	private PreparedStatement selPrepStmt;
	private Long id;
	private String curStatus;
	private PreparedStatement cntCrPrepStmt;
	private PreparedStatement cntDelPrepStmt;
	private ResultSet reSet;
	private PreparedStatement upPrepStmt;

	@Override
	public void handle(List<JobStatus> jobStatuses) {
		try {
			//load the driver and get connection
			Class.forName("com.mysql.jdbc.Driver");
			connect = DriverManager.getConnection(url);
			connect.setAutoCommit(false);
			
			String stmt = "insert into  jobs(jobid,cluster,user,start_time,end_time,duration,name,status,notes,estimated_time,map_progress,reduce_progress,created_at,updated_at)" +
				" values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			crPrepStmt = connect.prepareStatement(stmt);
			selPrepStmt = connect.prepareStatement("select id, status from jobs where cluster=? and jobid=?");
			cntCrPrepStmt = connect.prepareStatement("insert into metrics(context,category,name,value,job_id,created_at,updated_at) values(?,?,?,?,?,?,?)");
			String upStmt = "update jobs set start_time=?, end_time=?, duration=?, status=?, map_progress=?, reduce_progress=?, update_at=? where id=?";
			upPrepStmt = connect.prepareStatement(upStmt);
			cntDelPrepStmt = connect.prepareStatement("delete from metrics where job_id=?");
			
			Timestamp curDateTime = new Timestamp(System.currentTimeMillis());
			
			int count = 1;
	        for (JobStatus jobStatus : jobStatuses){
				System.out.println("job: " + count);
	        	String jobId = jobStatus.getJobId();
	        	String cluster = jobStatus.getCluster();
	        	getJob(cluster, jobId);
	        	if (null == id){
	        		//create job
		        	crPrepStmt.setString(1, jobId);
		        	crPrepStmt.setString(2, cluster);
		        	crPrepStmt.setString(3, jobStatus.getUser());
		        	crPrepStmt.setTimestamp(4, new Timestamp(jobStatus.getStartTime()));
		        	crPrepStmt.setTimestamp(5, new Timestamp(jobStatus.getEndTime()));
		        	crPrepStmt.setLong(6, jobStatus.getDuration());
		        	crPrepStmt.setString(7, jobStatus.getJobName());
		        	crPrepStmt.setString(8, jobStatus.getStatus());
		        	crPrepStmt.setString(9, jobStatus.getNotes());
		        	crPrepStmt.setString(10, null);
		        	crPrepStmt.setLong(11, jobStatus.getMapProgress());
		        	crPrepStmt.setLong(12, jobStatus.getReduceProgress());
		        	crPrepStmt.setTimestamp(13, curDateTime);
		        	crPrepStmt.setTimestamp(14, curDateTime);
		        	crPrepStmt.executeUpdate();
					System.out.println("job saved");

					//create counters
		        	getJob(cluster, jobId);
		        	createCounters(jobStatus, curDateTime);
		        	
					System.out.println("job counters saved");
					++count;
	        	} else {
	        		//update job
	        		upPrepStmt.setTimestamp(1, new Timestamp(jobStatus.getStartTime()));
	        		upPrepStmt.setTimestamp(2, new Timestamp(jobStatus.getEndTime()));
	        		upPrepStmt.setLong(3, jobStatus.getDuration());
	        		upPrepStmt.setString(4, jobStatus.getStatus());
	        		upPrepStmt.setLong(5, jobStatus.getMapProgress());
	        		upPrepStmt.setLong(6, jobStatus.getReduceProgress());
	        		upPrepStmt.setTimestamp(7, curDateTime);
	        		upPrepStmt.setLong(8, id);
	        		upPrepStmt.executeUpdate();
	        		
	        		//delete and create counters
	        		cntDelPrepStmt.setLong(1, id);
	        		cntDelPrepStmt.executeUpdate();
	        		createCounters(jobStatus, curDateTime);
	        	}
	        }
			connect.commit();
		} catch (Exception ex) {
			try {
				connect.rollback();
			} catch (SQLException sqEx) {
				System.out.println("Exception rolling back" + sqEx);
			}
			System.err.println("Failed in rdbms consumer: " + ex);
		} finally {
			try {
				if (null != reSet){
					reSet.close();
				}
				if (null != selPrepStmt){
					crPrepStmt.close();
				}
				if (null != crPrepStmt){
					crPrepStmt.close();
				}
				if (null != upPrepStmt){
					upPrepStmt.close();
				}
				if (null != cntCrPrepStmt){
					cntCrPrepStmt.close();
				}
				if (null != cntDelPrepStmt){
					cntDelPrepStmt.close();
				}
				if (null != connect){
					connect.close();
				}
			} catch (SQLException sqEx){
				System.out.println("Exception closing db resource" + sqEx);
			}
		}
	}
	
	private void getJob(String cluster, String jobId) throws Exception{
		reSet = null;
        id = null;
		curStatus = null;
		selPrepStmt.setString(1, cluster);
		selPrepStmt.setString(2, jobId);
		reSet = selPrepStmt.executeQuery();
		if (reSet.next()) {
			id = reSet.getLong(1);
			curStatus = reSet.getString(2);
		}
	}
	
	private void createCounters(JobStatus jobStatus, Timestamp curDateTime) throws Exception {
    	cntCrPrepStmt.setString(1, "job");
    	cntCrPrepStmt.setLong(5, id);
    	for(JobCounterGroup cntGrp : jobStatus.getCounterGroups()) {
        	cntCrPrepStmt.setString(2, cntGrp.getName());
        	for (JobCounterGroup.JobCounter cnt  : cntGrp.getJobCounters()) {
	        	cntCrPrepStmt.setString(3, cnt.getName());
	        	cntCrPrepStmt.setLong(4, cnt.getValue());
	        	cntCrPrepStmt.setTimestamp(6, curDateTime);
	        	cntCrPrepStmt.setTimestamp(7, curDateTime);
	        	cntCrPrepStmt.executeUpdate();
        	}
    	}

	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

}
