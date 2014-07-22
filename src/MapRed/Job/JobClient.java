package MapRed.Job;

import java.text.NumberFormat;

import Utility.Communication;
import Utility.Message;
import Utility.Utility;
import Conf.Configuration;

public class JobClient {

	private static Communication jobTrackerComm;
	private static boolean isRunning = true;
	private static int nodeDone = 0;

	public static void runJob(Configuration conf) {
		NumberFormat format = NumberFormat.getPercentInstance();
		format.setMinimumFractionDigits(1);
		
		Utility.configure();
		jobTrackerComm = new Communication(Utility.JOBTRACKER.ipAddress,
				Utility.JOBTRACKER.port);
		System.out.println("create the mapreduce job...");
		/* Register this task tracker */
		Message msg = new Message(Utility.CONF, conf);

		jobTrackerComm.sendMessage(msg);

		/* Wait for incoming commands */
		while (isRunning) {
			msg = jobTrackerComm.readMessage();
			if (msg.getMsgType() == Utility.MAPPERDONE) {
				System.out.print("Completed mapper job: ");
				System.out.println(format.format((double) ++nodeDone / (double) Utility.TASKTRACKERS.size()));
				if(nodeDone == Utility.TASKTRACKERS.size()) nodeDone = 0;
			}else if(msg.getMsgType() == Utility.REDUCERDONE){
				System.out.print("Completed reducer job: ");
				System.out.println(format.format((double) ++nodeDone / (double) Utility.TASKTRACKERS.size()));
				if(nodeDone == Utility.TASKTRACKERS.size()) isRunning = false;
			}
		}
	}

}
