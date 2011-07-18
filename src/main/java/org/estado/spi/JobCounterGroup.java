package org.estado.spi;

import java.util.ArrayList;
import java.util.List;

public class JobCounterGroup {
	private String name;
	private List<JobCounter> jobCounters = new ArrayList<JobCounter>();
	
	
	public String getName() {
		return name;
	}


	public void setName(String name) {
		this.name = name;
	}


	public List<JobCounter> getJobCounters() {
		return jobCounters;
	}


	public void setJobCounters(List<JobCounter> jobCounters) {
		this.jobCounters = jobCounters;
	}
	
	public void addJobCounter(String name, long value){
		jobCounters.add(new JobCounter(name, value));
	}
	

	public static class JobCounter {
		private String name;
		private long value;
		
		public JobCounter(String name, long value) {
			super();
			this.name = name;
			this.value = value;
		}

		public String getName() {
			return name;
		}
		
		public void setName(String name) {
			this.name = name;
		}
		
		public long getValue() {
			return value;
		}
		
		public void setValue(long value) {
			this.value = value;
		}
	}

}
