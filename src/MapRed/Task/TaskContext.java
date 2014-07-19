package MapRed.Task;

import java.io.IOException;

import Conf.Configuration;
import MapRed.IStatusReporter;
import MapRed.Job.JobContext;
import MapRed.Output.IRecordWriter;
import MapRed.Output.OutputCommitter;

public abstract class TaskContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT> extends JobContext {

	private final TaskID taskId;
	private String status = "";
	private IRecordWriter<KEYOUT,VALUEOUT> output;
	private IStatusReporter reporter;
	private OutputCommitter committer;

	public TaskContext(Configuration conf, TaskID taskid,
			IRecordWriter<KEYOUT,VALUEOUT> output,
			OutputCommitter committer,
			IStatusReporter reporter) {
		super(conf, taskid.getJobID());
		this.taskId = taskid;
		this.output = output;
		this.reporter = reporter;
		this.committer = committer;
	}

	public abstract boolean nextKeyValue() throws IOException, InterruptedException;

	public abstract KEYIN getCurrentKey() throws IOException, InterruptedException;

	public abstract VALUEIN getCurrentValue() throws IOException, InterruptedException;

	
	public void write(KEYOUT key, VALUEOUT value
			) throws IOException, InterruptedException {
		output.write(key, value);
	}
	/*
	public Counter getCounter(Enum<?> counterName) {
		return reporter.getCounter(counterName);
	}

	public Counter getCounter(String groupName, String counterName) {
		return reporter.getCounter(groupName, counterName);
	}

	@Override
	public void progress() {
		reporter.progress();
	}

	@Override
	public void setStatus(String status) {
		reporter.setStatus(status);
	}*/

	public OutputCommitter getOutputCommitter() {
		return committer;
	}

	public TaskID getTaskID() {
		return taskId;
	}

	public String getStatus() {
		return status;
	}
}
