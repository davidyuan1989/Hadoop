package Test;

import java.io.IOException;
import java.net.ServerSocket;

import Conf.Configuration;
import MapRed.Job.JobContext;
import Utility.Communication;
import Utility.Message;
import Utility.Utility;

public class test {
	private static Communication jobTrackerComm;
	
	public static void main(String[] args) {
		/*
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				try {
					ServerSocket serverSocket = new ServerSocket(33150);
					serverSocket.accept();
					serverSocket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}	
			}
		}).start();
		*/
		Utility.configure();
		jobTrackerComm = 
				new Communication(Utility.JOBTRACKER.ipAddress, Utility.JOBTRACKER.port);
		Configuration configuration = new Configuration();
		JobContext jobContext = new JobContext(configuration, null);
		Message msg = new Message(Utility.NEWJOB, jobContext);
		jobTrackerComm.sendMessage(msg);
	}
}
