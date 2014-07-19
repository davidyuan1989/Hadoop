package JZFS;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import Utility.Communication;
import Utility.Message;
import Utility.Utility;

public class DataNode extends JZFSNode {

	public long spaceUsage = 0L;
	
	private static final long serialVersionUID = 5L;
	private List<String> files = new ArrayList<String>();

	public DataNode(InetAddress ipAddress, int port) {
		super(ipAddress, port);
	}

	@Override
	public void start() {
		try {
			doHeartBeat();
			
			ServerSocket server = new ServerSocket(port);

			while (isRunning) {
				final Socket socket = server.accept();
				new Thread(new Runnable() {

					@Override
					public void run() {
						Communication comm = new Communication(socket);
						dataNodeHandler(comm);
					}
				}).start();
			}

			server.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private long getTotalFilesSize() {

		long size = 0L;

		for (int i = 0; i < files.size(); i++) {
			File file = new File(files.get(i));
			size += file.length();
			//System.out.println("Size of file " + files.get(i) + " is " + file.length());
		}

		return size;
	}

	private void dataNodeHandler(Communication comm) {

		FileInputStream reader = null;
		FileOutputStream writer = null;

		final int BUFSIZE = 1024;
		byte[] data = new byte[BUFSIZE];

		try {
			while (true) {

				Message msg = comm.readMessage();

				/* Read from the file */
				if (msg.getMsgType() == Utility.READFILE) {
					/* Open the file if not opened */
					if (reader == null) {
						File file = new File(msg.getFileName());
						reader = new FileInputStream(file);
					}

					int size = reader.read(data);
					msg = new Message(Utility.DATAREAD, data, size);
					comm.sendMessage(msg);
				}

				/* Write to the file */
				else if (msg.getMsgType() == Utility.WRITEFILE) {
					/* Open the file if not opened */
					if (writer == null) {

						if (!files.contains(msg.getFileName())) {
							files.add(msg.getFileName());
							System.out.println("New file " + msg.getFileName() + " created in node ["
									+ ipAddress + ":" + port + "].");
						}

						File file = new File(msg.getFileName());
						writer = new FileOutputStream(file);
					}

					writer.write(msg.getData(), 0, msg.getDataSize());
					msg = new Message(Utility.ACK);
					comm.sendMessage(msg);
				}

				/* Close the file that opened */
				else if (msg.getMsgType() == Utility.CLOSEFILE) {
					if (reader != null) {
						reader.close();
					}

					if (writer != null) {
						writer.close();
					}

					break;
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/*
	 * doHeartBeat
	 * 		- Send heart beat message to the master
	 */
	private void doHeartBeat() {
		Timer timer = new Timer();

		TimerTask timerTask = new TimerTask() {
			@Override
			public void run() {
				spaceUsage = getTotalFilesSize();
				Communication namenodeComm = new Communication(Utility.NAMENODE.ipAddress, Utility.NAMENODE.port);
				Message msg = new Message(Utility.DATANODEHEARTBEAT, spaceUsage, id);
				namenodeComm.sendMessage(msg);
			}
		};

		timer.schedule(timerTask, 1000, 5000);
	}
}
