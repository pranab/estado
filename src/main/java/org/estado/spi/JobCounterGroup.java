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
