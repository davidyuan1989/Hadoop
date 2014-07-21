package MapRed.Job;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import Conf.Configuration;
import Utility.JZFile;

public class JobContext implements Serializable {

	private static final long serialVersionUID = 1L;

	public static enum JobStatus {INIT, MAPPING, REDUCING, FINISHED};
	
	protected final Configuration conf;
	private final JobID jobID;
	private JobStatus jobStatus = JobStatus.INIT;

	public JobContext(Configuration conf, JobID jobID) {
		this.conf = conf;
		this.jobID = jobID;
	}

	public Configuration getConfiguration() {
		return conf;
	}

	public JobID getJobID() {
		return jobID;
	}

	public long getMinSplitSize() {
		return 0L;
	}

	public long getMaxSplitSize() {
		return Long.MAX_VALUE;
	}

	public List<JZFile> getInputFiles() {
		return new ArrayList<JZFile>();
	}
	
	public void setJobStatus(JobStatus status) {
		jobStatus = status;
	}
	
	public JobStatus getJobStatus() {
		return jobStatus;
	}
}
