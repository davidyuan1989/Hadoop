package MapRed.Job;

import java.io.Serializable;

public class JobID implements Serializable{

	private static final long serialVersionUID = 2L;
	private static final String SEPARATOR = "_";
	private int jobTrackerID;
	private int jobID;
	private String ID;
	
	public JobID(int jobTrackerID, int jobID) {
		this.jobTrackerID = jobTrackerID;
		this.jobID = jobID;
		this.ID = createID(jobTrackerID, jobID);
	}
	
	public int getJobTrackerID() {
		return jobTrackerID;
	}
	
	public int getJobID() {
		return jobID;
	}
	
	public String getID() {
		return ID;
	}
	
	public static String createID(int jobTrackerID, int jobID) {
		String ID = "job" + SEPARATOR + String.valueOf(jobTrackerID) + 
					SEPARATOR + String.valueOf(jobID);
		return ID;
	}
}
