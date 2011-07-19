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

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 *
 * @author pranab
 */
public class JobStatusConsoleConsumer implements JobStatusConsumer {
    
    @Override
    public void handle(List<JobStatus> jobStatuses) {
    	System.out.println("Reporting on " + jobStatuses.size() + " jobs\n\n");
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Date date = Calendar.getInstance().getTime();

        int count = 1;
        for (JobStatus jobStatus : jobStatuses){
	        date.setTime(jobStatus.getStartTime());
	        String startTime = formatter.format(date);
	        
	        String endTime = "";
	        if (null != jobStatus.getEndTime()) {
	            date.setTime(jobStatus.getEndTime());
	            endTime = formatter.format(date);
	            
	        }
	        
	        String duration = "";
	        if (null != jobStatus.getDuration()) {
	        	duration = formattedTime(jobStatus.getDuration());
	        }
	        
	        
	        //job stat
	        System.out.println("\nJob:  " + count);
	        System.out.println("Cluster:" + jobStatus.getCluster());
	        System.out.println("Job Id:" + jobStatus.getJobId());
	        System.out.println("Job name:" + jobStatus.getJobName() != null ? jobStatus.getJobName() : "");
	        System.out.println("Notes:" + jobStatus.getNotes() != null ? jobStatus.getNotes() : "");
	        System.out.println("User:" + jobStatus.getUser());
	        System.out.println("Start time:" + startTime);
	        System.out.println("End time:" + endTime);
	        System.out.println("Duration:" + duration);
	        System.out.println("Map progress:" + jobStatus.getMapProgress());
	        System.out.println("Reduce progress:" + jobStatus.getReduceProgress());
	        System.out.println("Status:" + jobStatus.getStatus());
	        
	        //counters
	        for(JobCounterGroup counterGroup : jobStatus.getCounterGroups()){
		        System.out.println("Counter group:" + counterGroup.getName());
		        for (JobCounterGroup.JobCounter counter : counterGroup.getJobCounters()){
			        System.out.println("\t" + counter.getName() + "=" + counter.getValue());
		        }
	        }
	        
	        ++count;
    	}
    }
    
    private String formattedTime(long time){
        long milSec = time % 1000;
        time /= 1000;
        long sec = time % 60;
        time /= 60;
        long min = time % 60;
        time /= 60;
        long hour = time % 24;
        StringBuilder stBuilder = new StringBuilder();
        if (hour > 0 ){
            stBuilder.append(formatField(hour, 2)).append("hour:");
        }
        if (min > 0){
            stBuilder.append(formatField(min, 2)).append("min:");
        }
        if (sec > 0){
            stBuilder.append(formatField(sec, 2)).append("sec:");
        }
        if (milSec > 0){
            stBuilder.append(formatField(milSec, 3)).append("ms");
        }
        return stBuilder.toString();
    }
    
    private String formatField(long value, int size){
    	String stValue = null;
    	if (2 == size){
    		stValue = value < 10 ? "0" + value : "" + value;
    	} else {
    		stValue = value < 10 ? "00" + value : (value < 100 ? "0" + value : "" + value);
    	}
    	return stValue;
    }
    

}
