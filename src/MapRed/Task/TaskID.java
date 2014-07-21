package MapRed.Task;

import java.io.Serializable;

import MapRed.Job.JobID;

public class TaskID implements Serializable {

	private static final long serialVersionUID = 3L;

	public static enum TaskType {MAPPER_T, COMBINER_T, REDUCER_T};
	
	private static final String SEPARATOR = "_";
	private JobID jobID;
	private TaskType taskType;
	private int taskID;
	private int taskAttemptID;
	private String ID;
	
	public TaskID(JobID jobID, TaskType taskType, int taskID, int taskAttemptID) {
		this.jobID = jobID;
		this.taskType = taskType;
		this.taskID = taskID;
		this.taskAttemptID = taskAttemptID;
		this.ID = createID(jobID, taskType, taskID, taskAttemptID);
	}
	
	public JobID getJobID() {
		return jobID;
	}
	
	public TaskType getTaskType() {
		return taskType;
	}
	
	public int getTaskID() {
		return taskID;
	}
	
	public int getTaskAttemptID() {
		return taskAttemptID;
	}
	
	public String getID() {
		return ID;
	}
	
	public static String createID(JobID jobID, TaskType taskType, 
								int taskID, int taskAttemptID) {
		String retString;
		
		retString = "Task" + SEPARATOR + jobID.getID();
		
		if (taskType == TaskType.MAPPER_T) {
			retString += SEPARATOR + "M";
		}
		else if (taskType == TaskType.REDUCER_T) {
			retString += SEPARATOR + "R";
		}
		else if (taskType == TaskType.COMBINER_T) {
			retString += SEPARATOR + "C";
		}
		
		retString += SEPARATOR + String.valueOf(taskID) + SEPARATOR + String.valueOf(taskAttemptID);
		
		return retString;
	}
}
