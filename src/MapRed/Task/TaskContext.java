package MapRed.Task;

import java.io.IOException;

import Conf.Configuration;
import MapRed.Job.JobContext;

public class TaskContext extends JobContext {

	private static final long serialVersionUID = 4L;

	public static enum TaskStatus {INIT, INPROGRESS, FINISHED};
	
	private final TaskID taskId;
	private TaskStatus taskStatus = TaskStatus.INIT;

	public TaskContext(Configuration conf, TaskID taskid) {
		super(conf, taskid.getJobID());
		this.taskId = taskid;
	}

	public TaskID getTaskID() {
		return taskId;
	}

	public TaskStatus getTaskStatus() {
		return taskStatus;
	}

	public void setTaskStatus(TaskStatus taskStatus) throws IOException {
		this.taskStatus = taskStatus;
	}
}
