/*
 * Class Name: ClientCommunication
 * 		- Handle client side communication, including connecting to the
 * 		  server, reading from and writing to the server
 * Author: Jianan Lu (jiananl) & Zizhou Deng (zdeng)
 * 
 */

package Utility;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;


public class Communication {
	
	/* Variables used to buffer the connection with the server */
	public Socket client;					/* Client side socket */
	private ObjectOutputStream outputStream;/* Output stream of the stream */
	private ObjectInputStream inputStream;	/* Input stream of the stream */
	
	public Communication(InetAddress serverAddress, int port) {
		
		/* Establish connection with the server */
		try {
			client = new Socket(serverAddress, port);
			outputStream = new ObjectOutputStream(client.getOutputStream());
			inputStream = new ObjectInputStream(client.getInputStream());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public Communication(Socket socket) {
		try {
			client = socket;
			outputStream = new ObjectOutputStream(client.getOutputStream());
			inputStream = new ObjectInputStream(client.getInputStream());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/*
	 * sendMessage
	 * 		- Send a message to the server and return the reply message from the server
	 */
	public void sendMessage(Message msg) {
		
		try {
			outputStream.writeObject(msg);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public Message readMessage() {
		
		Message retMessage = null;
		
		try {
			retMessage = (Message) inputStream.readObject();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		return retMessage;
	}
	
	/*
	 * close
	 * 		- Close the buffered connection with the server
	 */
	public void close() {
		
		if (client != null) {
			
			try {
				inputStream.close();
				outputStream.close();
				client.close();
			} catch (IOException e) {
				e.printStackTrace();
			}		
		}
	}
}
