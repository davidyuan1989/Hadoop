package JZFS;
/*
 * Class Name: JZFSFileInputStream
 * 		- JZFS input class
 * Author: Jianan Lu (jiananl) & Zizhou Deng (zdeng)
 * 
 */

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;

import Utility.Communication;
import Utility.Message;
import Utility.Utility;


public class JZFSFileInputStream extends InputStream{

	/**
	 *	Instance variables
	 */
	private final int BUFSIZE = 1024;
	private String fileName;					/* Input file name */
	private Communication datanodeComm;			/* Communicator with the Namenode */
	private byte[] data = new byte[BUFSIZE];	/* Buffer of data */
	private int sizeOfBuf = 0;					/* Number of bytes of data stored in the buffer */
	private int posOfBuf = 0;					/* Current position in the buffer */
	
	/* Constructor */
	public JZFSFileInputStream(String fileName) throws NoSuchFileException {
		
		this.fileName = fileName;
		
		Communication namenodeComm = new Communication(Utility.NAMENODE.ipAddress, Utility.NAMENODE.port);
		Message msg = new Message(Utility.READFILE, this.fileName);
		namenodeComm.sendMessage(msg);
		msg = namenodeComm.readMessage();
		
		if (msg.getMsgType() == Utility.FILELOCATION) {
			datanodeComm = new Communication(msg.getDataNodeMachine().ipAddress, msg.getDataNodeMachine().port);
		}
		else if (msg.getMsgType() == Utility.NOSUCHFILE) {
			throw new NoSuchFileException(fileName);
		}
		
		namenodeComm.close();
	}

	@Override
	public int read() throws IOException {
		
		/* Return the current byte in the data array */
		if (posOfBuf < sizeOfBuf) {
			return data[posOfBuf++];
		}
		
		/* Empty array, then read from the file system */
		else {
			posOfBuf = 0;
			readBuf();
			if (sizeOfBuf != -1) {
				return data[posOfBuf++];
			}
			else {
				return -1;
			}
		}
	}
	
	public void close() {
		Message msg = new Message(Utility.CLOSEFILE, fileName);
		datanodeComm.sendMessage(msg);
		datanodeComm.close();
	}
	
	private void readBuf() {
		
		Message msg = new Message(Utility.READFILE, fileName);
		datanodeComm.sendMessage(msg);
		msg = datanodeComm.readMessage();
		
		if (msg.getMsgType() == Utility.DATAREAD) {
			data = msg.getData();
			sizeOfBuf = msg.getDataSize();
		}
	}
}
