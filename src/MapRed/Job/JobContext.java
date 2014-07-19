package MapRed.Job;

import java.util.ArrayList;
import java.util.List;

import Conf.Configuration;
import Utility.JZFile;

public class JobContext {

	protected final Configuration conf;
	private final JobID jobID;

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
}
