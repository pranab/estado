/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.estado.spi;

import java.util.List;

/**
 *
 * @author pranab
 */
public interface JobStatusConsumer {
    public void handle(List<JobStatus> jobStatuses);
}
