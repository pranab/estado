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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.estado.spi.JobStatusConsumer;

public class ConsumerConfigurator {
	
	public List<JobStatusConsumer> buildConsumers(Properties configProps) throws Exception {
		List<JobStatusConsumer> consumers = new ArrayList<JobStatusConsumer>();
		Map<String,Configuration> configs = new HashMap<String, Configuration>();
		 
		for (Object keyObj : configProps.keySet()){
			String key = keyObj.toString();
			String[] items = key.split("\\.");
			if (items[0].equals("consumer")){
				String name = items[1];
				Configuration config = configs.get(name);
				if (null == config){
					config = new Configuration(name);
					configs.put(name, config);
				}
				
				if (items[2].equals("class")) {
					String clazz = configProps.getProperty(key);
					config.setClazz(clazz);
				} else {
					String propName = items[2];
					String propValue = configProps.getProperty(key);
					config.addProperty(propName, propValue);
				}
			}
		}
		
		for (String consName : configs.keySet()){
			Configuration config = configs.get(consName);
			JobStatusConsumer  consumer = config.buildConsumer();
			consumers.add(consumer);
		}
		
		return consumers;
	}
	
	public static class Configuration {
		private String name;
		private String clazz;
		private Map<String, String> props = new HashMap<String, String>();
		
		public Configuration(String name){
			this.name = name;
		}
		
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public String getClazz() {
			return clazz;
		}
		public void setClazz(String clazz) {
			this.clazz = clazz;
		}
		public Map<String, String> getProps() {
			return props;
		}
		public void setProps(Map<String, String> props) {
			this.props = props;
		}
		
		public void addProperty(String name, String value){
			props.put(name, value);
		}
		
		public JobStatusConsumer  buildConsumer() throws Exception {
			JobStatusConsumer consumer = null;
            Class<?> consumerCls = Class.forName(clazz);
            consumer = (JobStatusConsumer)consumerCls.newInstance();
            
            for (String propName : props.keySet()){
            	String propValue = props.get(propName);
            	String methodName = "set" + Character.toUpperCase(propName.charAt(0)) +
            		propName.substring(1);
            	Class[] types = new Class[] {String.class};
                Method method = consumerCls.getMethod(methodName, types);
                method.invoke(consumer, new Object[] {propValue});
            }
			return consumer;
		}
	}

}
