/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.estado.spi;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author pranab
 */
public class JobStatus {
	private String cluster;
    private String jobId;
    private String user;
    private Long startTime;
    private Long endTime;
    private Long duration;
    private int mapProgress;
    private int reduceProgress;
    private String status;
    private List<JobCounterGroup> counterGroups = new ArrayList<JobCounterGroup>();

    public JobStatus(String cluster, String jobId, String user, Long startTime,
			Long endTime, Long duration, int mapProgress, int reduceProgress,
			String status) {
		super();
		this.cluster = cluster;
		this.jobId = jobId;
		this.user = user;
		this.startTime = startTime;
		this.endTime = endTime;
		this.duration = duration;
		this.mapProgress = mapProgress;
		this.reduceProgress = reduceProgress;
		this.status = status;
	}

	/**
     * @return the jobId
     */
    public String getJobId() {
        return jobId;
    }

    /**
     * @param jobId the jobId to set
     */
    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    /**
     * @return the user
     */
    public String getUser() {
        return user;
    }

    /**
     * @param user the user to set
     */
    public void setUser(String user) {
        this.user = user;
    }

    /**
     * @return the startTime
     */
    public Long getStartTime() {
        return startTime;
    }

    /**
     * @param startTime the startTime to set
     */
    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    /**
     * @return the endTime
     */
    public Long getEndTime() {
        return endTime;
    }

    /**
     * @param endTime the endTime to set
     */
    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

    /**
     * @return the duration
     */
    public Long getDuration() {
        return duration;
    }

    /**
     * @param duration the duration to set
     */
    public void setDuration(Long duration) {
        this.duration = duration;
    }

    /**
     * @return the status
     */
    public String getStatus() {
        return status;
    }

    /**
     * @param status the status to set
     */
    public void setStatus(String status) {
        this.status = status;
    }

	public String getCluster() {
		return cluster;
	}

	public void setCluster(String cluster) {
		this.cluster = cluster;
	}

	public int getMapProgress() {
		return mapProgress;
	}

	public void setMapProgress(int mapProgress) {
		this.mapProgress = mapProgress;
	}

	public int getReduceProgress() {
		return reduceProgress;
	}

	public void setReduceProgress(int reduceProgress) {
		this.reduceProgress = reduceProgress;
	}

	public List<JobCounterGroup> getCounterGroups() {
		return counterGroups;
	}

	public void setCounterGroups(List<JobCounterGroup> counterGroups) {
		this.counterGroups = counterGroups;
	}

}
