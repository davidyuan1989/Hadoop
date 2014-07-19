package JZFS;
/*
 * Class Name: JZFSFileOutputStream
 * 		- JZFS output class
 * Author: Jianan Lu (jiananl) & Zizhou Deng (zdeng)
 * 
 */

import java.io.IOException;
import java.io.OutputStream;

import Utility.Communication;
import Utility.Message;
import Utility.Utility;


public class JZFSFileOutputStream extends OutputStream {

	/**
	 * 
	 */
	private final int BUFSIZE = 1024;
	private String fileName;					/* Name of output file */
	private Communication datanodeComm;			/* Communicator with the Namenode */
	private byte[] data = new byte[BUFSIZE];	/* Buffer of data */
	private int posOfBuf = 0;					/* Current position in the buffer */

	/* Constructor */
	public JZFSFileOutputStream(String fileName) {
		this.fileName = fileName;
		
		Communication namenodeComm = new Communication(Utility.NAMENODE.ipAddress, Utility.NAMENODE.port);
		Message msg = new Message(Utility.WRITEFILE, this.fileName);
		namenodeComm.sendMessage(msg);
		msg = namenodeComm.readMessage();
		
		if (msg.getMsgType() == Utility.FILELOCATION) {
			datanodeComm = new Communication(msg.getDataNodeMachine().ipAddress, msg.getDataNodeMachine().port);
		}
		
		namenodeComm.close();
	}
	
	@Override
	public void write(int arg0) throws IOException {
		
		if (posOfBuf < BUFSIZE) {
			data[posOfBuf++] = (byte) arg0;
		}
		else {
			writeBuf();
			posOfBuf = 0;
		}
	}
	
	public void close() {
		Message msg = new Message(Utility.WRITEFILE, fileName, data, posOfBuf);
		datanodeComm.sendMessage(msg);
		msg = datanodeComm.readMessage();
		if (msg.getMsgType() == Utility.ACK) {
			msg = new Message(Utility.CLOSEFILE, fileName);
			datanodeComm.sendMessage(msg);
		}
		datanodeComm.close();
	}
	
	private void writeBuf() {
		Message msg = new Message(Utility.WRITEFILE, fileName, data, posOfBuf);
		datanodeComm.sendMessage(msg);
		msg = datanodeComm.readMessage();		/*ACK*/
	}
}
