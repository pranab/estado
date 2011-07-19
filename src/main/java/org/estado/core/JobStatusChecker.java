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

package org.estado.core;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.TaskReport;
import org.estado.spi.JobCounterGroup;
import org.estado.spi.JobStatusConsumer;

/**
 *
 * @author Pranab
 */
public class JobStatusChecker {
    private String filter = "";
    private int sCount = 0;
    private int rCount = 0;
    private int fCount = 0;
    private int pCount = 0;
    private int kCount = 0;
    private String cluster;
    private List<JobStatusConsumer> consumers;
    
    public JobStatusChecker(String filter, String cluster,
			List<JobStatusConsumer> consumers) {
		super();
		this.filter = filter;
		this.cluster = cluster;
		this.consumers = consumers;
	}

	public void checkStatus(){
        List<org.estado.spi.JobStatus> jobStatusList = new ArrayList<org.estado.spi.JobStatus>();
        
        try {
            Configuration conf = new Configuration();
            JobClient client = new JobClient(new JobConf(conf));
            JobStatus[] jobStatuses = client.getAllJobs();
            showFilter();

            int jobCount = 0;
            for (JobStatus jobStatus : jobStatuses) {
                Long lastTaskEndTime = 0L;
                TaskReport[] mapReports = client.getMapTaskReports(jobStatus.getJobID());
                for (TaskReport r : mapReports) {
                    if (lastTaskEndTime < r.getFinishTime()) {
                        lastTaskEndTime = r.getFinishTime();
                    }
                }
                TaskReport[] reduceReports = client.getReduceTaskReports(jobStatus.getJobID());
                for (TaskReport r : reduceReports) {
                    if (lastTaskEndTime < r.getFinishTime()) {
                        lastTaskEndTime = r.getFinishTime();
                    }
                }
                client.getSetupTaskReports(jobStatus.getJobID());
                client.getCleanupTaskReports(jobStatus.getJobID());
                
                String jobId = jobStatus.getJobID().toString();
                Long startTime = jobStatus.getStartTime();
                String user = jobStatus.getUsername();
                int mapProgress = (int)(jobStatus.mapProgress() * 100);
                int reduceProgress = (int)(jobStatus.reduceProgress() * 100);
                org.estado.spi.JobStatus jobStat = null;
                ++jobCount;

                int runState = jobStatus.getRunState();
                switch(runState){
                    case JobStatus.SUCCEEDED:
                        if (filter.contains("s")){
                            Long duration = lastTaskEndTime - jobStatus.getStartTime();
                            jobStat = new org.estado.spi.JobStatus(cluster, jobId, null, null, user, startTime, lastTaskEndTime,  
                            		duration, mapProgress, reduceProgress, "completed");
                            ++sCount;
                        }
                        break;

                    case JobStatus.RUNNING:
                        if (filter.contains("r")){
                            long duration = System.currentTimeMillis() - jobStatus.getStartTime();
                            jobStat = new org.estado.spi.JobStatus(cluster, jobId, null, null, user, startTime,  
                            		lastTaskEndTime,  duration, mapProgress, reduceProgress,"running");
                            ++rCount;
                        }
                        break;

                    case JobStatus.FAILED:
                        if (filter.contains("f")){
                            long duration = lastTaskEndTime - jobStatus.getStartTime();
                            jobStat = new org.estado.spi.JobStatus(cluster, jobId, null, null, user, startTime,
                            		lastTaskEndTime,  duration, mapProgress, reduceProgress, "failed");
                            ++fCount;
                        }
                        break;

                    case JobStatus.PREP:
                        if (filter.contains("p")){
                            jobStat = new org.estado.spi.JobStatus(cluster, jobId, null, null, user, null,
                            		null,  null, 0, 0, "preparing");
                            ++pCount;
                        }
                        break;
                        
                    case JobStatus.KILLED:
                        if (filter.contains("k")){
                            long duration = lastTaskEndTime - jobStatus.getStartTime();
                            jobStat = new org.estado.spi.JobStatus(cluster, jobId, null, null, user, startTime,
                            		lastTaskEndTime,  duration, mapProgress, reduceProgress, "killed");
                            ++kCount;
                        }
                        break;
                }

                jobStatusList.add(jobStat);
            }
            
            //get counters
            for (org.estado.spi.JobStatus jobStat : jobStatusList){
            	if (!jobStat.getStatus().equals("preparing")){
            		List<JobCounterGroup>  counterGroups = getJobCounters(jobStat.getJobId());
            		jobStat.setCounterGroups(counterGroups);
            		
            		//additional data from counters
            		setJobInfo(jobStat);
            	}
            }
            
            //publish to all consumers
            for (JobStatusConsumer consumer : consumers){
            	consumer.handle(jobStatusList);
            }
            
            showJobCounts();
        } catch (Exception ex) {
            System.out.println("Jobs status checker failed" + ex.getMessage());
        }

    }

    private void showFilter(){
        StringBuilder stBuilder = new StringBuilder("Reporting on ");
        if (filter.contains("s")){
            stBuilder.append(" succeeded ");
        }
        if (filter.contains("r")){
            stBuilder.append(" running ");
        }
        if (filter.contains("f")){
            stBuilder.append(" failed ");
        }
        if (filter.contains("p")){
            stBuilder.append(" preparing ");
        }
        if (filter.contains("k")){
            stBuilder.append(" killed ");
        }
        stBuilder.append(" jobs");
        System.out.println(stBuilder.toString());
    }

    private void showJobCounts(){
        if (filter.contains("s")){
            System.out.println("Found " + sCount + " completed jobs");
        }
        if (filter.contains("r")){
            System.out.println("Found " + rCount + " running jobs");
        }
        if (filter.contains("f")){
            System.out.println("Found " + fCount + " failed jobs");
        }
        if (filter.contains("p")){
            System.out.println("Found " + pCount + " preparing jobs");
        }
        if (filter.contains("k")){
            System.out.println("Found " + kCount + " killed jobs");
        }
    }

    private List<JobCounterGroup>  getJobCounters(String jobId) throws Exception {
    	List<JobCounterGroup> counterGroups = new ArrayList<JobCounterGroup>();
        
    	//shell command
    	String cmd = "hadoop job -status " + jobId;
        Process process = Runtime.getRuntime().exec(cmd);
        process.waitFor();
        BufferedReader buf = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line = "";
        List<String> lines = new ArrayList<String>();
        while ((line=buf.readLine())!=null) {
            lines.add(line);
        }
    	
        //parse output
        JobCounterGroup counterGroup = null;
        int i = 0;
        boolean found = false;
        for ( ; i < lines.size(); ++i){
        	if (lines.get(i).trim().startsWith("Counters:")){
        		found = true;
        		break;
        	}
        }
        
        if (found){
	        for (++i ; i < lines.size(); ++i){
	        	line = lines.get(i).trim();
	        	String[] items = line.split("=");
	        	if (items.length == 1){
	        		//start of new group
	        		counterGroup = new JobCounterGroup();
	        		counterGroup.setName(line);
	        		counterGroups.add(counterGroup);
	        	} else {
	        		//another counter in current group
	        		counterGroup.addJobCounter(items[0], new Long(items[1]));
	        	}
	        }
        } else {
        	System.err.println("Error parsing counters");
        }
        
    	return counterGroups;
    }
    
    
    private void setJobInfo(org.estado.spi.JobStatus jobStat){
    	List<JobCounterGroup>  counterGroups = jobStat.getCounterGroups();
    	
    	String[] items = null;
    	for (JobCounterGroup counterGroup : counterGroups){
    		if (counterGroup.getName().equals("JobInfo")) {
    			for (JobCounterGroup.JobCounter counter : counterGroup.getJobCounters()){
    				if (counter.getName().startsWith("jobName")){
    					items = counter.getName().split(":");
    					jobStat.setJobName(items[1]);
    				}
    				if (counter.getName().startsWith("notes")){
    					items = counter.getName().split(":");
    					jobStat.setNotes(items[1]);
    				}
    			}
    			break;
    		}
    	}
    }

    public static void main(String[] args){
        String confFilePath = args[0];
        try {
        	    if (null != confFilePath){
	            FileInputStream fis = new FileInputStream(confFilePath);
	            Properties configProps = new Properties();
	            configProps.load(fis);
	            
	            String cluster= configProps.getProperty("cluster.name", "default");
	            String filter= configProps.getProperty("status.filter", "srfpk");
	            if (filter.endsWith("*")){
	            	filter = "srfpk";
	            }
	            
	            ConsumerConfigurator consConfig = new ConsumerConfigurator();
	            List<JobStatusConsumer> consumers = consConfig.buildConsumers(configProps);
	            
	            JobStatusChecker statusChecker = new JobStatusChecker(filter, cluster, consumers);
	            statusChecker.checkStatus();
	        }
        } catch (Exception ex){
        	System.err.println("Error starting " + ex);
        }
    	
    }
}
