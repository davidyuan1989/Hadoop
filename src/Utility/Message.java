/*
 * Class Name: Message
 * 		- Data structure that represents the messages transferred between server
 * 		  and clients
 * Author: Jianan Lu (jiananl) & Zizhou Deng (zdeng)
 * 
 */

package Utility;

import java.io.Serializable;
import java.util.List;

import MapRed.Job.JobContext;
import MapRed.Map.MapBasicContext;
import MapRed.Reduce.ReduceBasicContext;

public class Message implements Serializable {

	private static final long serialVersionUID = 1L;

	private int type = 0;		/* Type of the message */

	private String fileName;	/* Name of the file to read */
	
	private Machine datanodeMachine;

	private byte[] data;		/* Array of data that read */
	private int size;			/* Size of the data that read */
	
	private long datanodeSpaceUsed;
	private String nodeID;
	
	private int taskTrackerID;
	private JobContext jobContext;
	private List<MapBasicContext> mapContexts;
	private List<ReduceBasicContext> reduceContexts;
	
	private String jobID;

	public Message(int type) {

		if (type == Utility.ACK || type == Utility.TASKTRACKERREG || type == Utility.NEWJOBACK) {
			this.type = type;
		}

		else {
			System.out.println("Message type and arguments do not match! Message requested: " + type);
		}
	}

	public Message(int type, String string) {

		if (type == Utility.READFILE || type == Utility.CLOSEFILE || type == Utility.WRITEFILE) {
			this.type = type;
			this.fileName = string;
		}

		else {
			System.out.println("Message type and arguments do not match! Message requested: " + type);
		}
	}
	
	public Message(int type, int taskTrackerID) {

		if (type == Utility.REGACK) {
			this.type = type;
			this.taskTrackerID = taskTrackerID;
		}

		else {
			System.out.println("Message type and arguments do not match! Message requested: " + type);
		}
	}
	
	public Message(int type, JobContext jobContext) {

		if (type == Utility.NEWJOB) {
			this.type = type;
			this.jobContext = jobContext;
		}

		else {
			System.out.println("Message type and arguments do not match! Message requested: " + type);
		}
	}
	
	@SuppressWarnings("unchecked")
	public Message(int type, List<?> contexts) {

		if (type == Utility.RUNMAPPER) {
			this.type = type;
			this.mapContexts = (List<MapBasicContext>) contexts;
		}
		
		else if (type == Utility.RUNREDUCER) {
			this.type = type;
			this.reduceContexts = (List<ReduceBasicContext>) contexts;
		}

		else {
			System.out.println("Message type and arguments do not match! Message requested: " + type);
		}
	}
	
	public Message(int type, Machine machine) {

		if (type == Utility.FILELOCATION) {
			this.type = type;
			this.datanodeMachine = machine;
		}

		else {
			System.out.println("Message type and arguments do not match! Message requested: " + type);
		}
	}

	public Message(int type, int taskTrackerID, String jobID) {

		if (type == Utility.MAPPERDONE || type == Utility.REDUCERDONE) {
			this.type = type;
			this.taskTrackerID = taskTrackerID;
			this.jobID = jobID;
		}

		else {
			System.out.println("Message type and arguments do not match! Message requested: " + type);
		}
	}
	
	public Message(int type, byte[] data, int size) {

		if (type == Utility.DATAREAD) {
			this.type = type;
			this.data = data;
			this.size = size;
		}

		else {
			System.out.println("Message type and arguments do not match! Message requested: " + type);
		}
	}
	
	public Message(int type, String fileName, byte[] data, int size) {

		if (type == Utility.WRITEFILE) {
			this.type = type;
			this.fileName = fileName;
			this.data = data;
			this.size = size;
		}

		else {
			System.out.println("Message type and arguments do not match! Message requested: " + type);
		}
	}
	
	public Message(int type, long datanodeSpaceUsed, String nodeID) {

		if (type == Utility.DATANODEHEARTBEAT) {
			this.type = type;
			this.datanodeSpaceUsed = datanodeSpaceUsed;
			this.nodeID = nodeID;
		}

		else {
			System.out.println("Message type and arguments do not match! Message requested: " + type);
		}
	}
	
	public int getMsgType() {
		return type;
	}

	public String getFileName() {
		return fileName;
	}
	
	public Machine getDataNodeMachine() {
		return datanodeMachine;
	}

	public byte[] getData() {
		return data;
	}

	public int getDataSize() {
		return size;
	}
	
	public long getDataNodeSpaceUsed() {
		return datanodeSpaceUsed;
	}
	
	public String getNodeID() {
		return nodeID;
	}
	
	public int getTaskTrackerID() {
		return taskTrackerID;
	}
	
	public JobContext getJobContext() {
		return jobContext;
	}
	
	public List<MapBasicContext> getMapContexts() {
		return mapContexts;
	}
	
	public List<ReduceBasicContext> getReduceContexts() {
		return reduceContexts;
	}
	
	public String getJobID() {
		return jobID;
	}
}
