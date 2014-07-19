/*
 * Class Name: Message
 * 		- Data structure that represents the messages transferred between server
 * 		  and clients
 * Author: Jianan Lu (jiananl) & Zizhou Deng (zdeng)
 * 
 */

package Utility;

import java.io.Serializable;

public class Message implements Serializable {

	private static final long serialVersionUID = 1L;

	private int type = 0;		/* Type of the message */

	private String fileName;	/* Name of the file to read */
	
	private Machine datanodeMachine;

	private byte[] data;		/* Array of data that read */
	private int size;			/* Size of the data that read */
	
	private long datanodeSpaceUsed;
	private String nodeID;

	public Message(int type) {

		if (type == Utility.ACK) {
			this.type = type;
		}

		else {
			System.out.println("Message type and arguments do not match! Message requested: " + type);
		}
	}

	public Message(int type, String fileName) {

		if (type == Utility.READFILE || type == Utility.CLOSEFILE || type == Utility.WRITEFILE) {
			this.type = type;
			this.fileName = fileName;
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
}
