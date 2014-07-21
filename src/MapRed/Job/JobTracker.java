package MapRed.Job;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import Conf.Configuration;
import MapRed.Input.FileSplit;
import MapRed.Input.IInputSplit;
import MapRed.Map.MapBasicContext;
import MapRed.Reduce.ReduceBasicContext;
import MapRed.Task.TaskID;
import MapRed.Task.TaskID.TaskType;
import Utility.Communication;
import Utility.JZFile;
import Utility.Message;
import Utility.Utility;

public class JobTracker {

	private static enum TaskTrackerJobStatus {INIT, MAPPING, REDUCING, FINISHED};
	
	private static class TaskTrackerContext {
		public Map<String, TaskTrackerJobStatus> jobStatuses = 
				new HashMap<String, TaskTrackerJobStatus>();
		public Map<String, List<MapBasicContext>> jobMappers = 
				new HashMap<String, List<MapBasicContext>>();
		public Map<String, List<ReduceBasicContext>> jobReducers = 
				new HashMap<String, List<ReduceBasicContext>>();
	}
	
	private static Map<Integer, TaskTrackerContext> taskTrackerContexts = 
			new HashMap<Integer, TaskTrackerContext>();
	
	private static int taskTrackerIDPool = 0;
	private static boolean isRunning = true;

	private static Map<String, JobContext> jobContexts = 
			new HashMap<String, JobContext>();
	private static Map<String, List<MapBasicContext>> jobMappers = 
			new HashMap<String, List<MapBasicContext>>();
	private static Map<String, List<ReduceBasicContext>> jobReducers = 
			new HashMap<String, List<ReduceBasicContext>>();

	private static List<Communication> taskTrackerComms = 
			new ArrayList<Communication>();

	public static void main(String[] args) {
		try {
			Utility.configure();
			
			/* Receive registration from task trackers */
			ServerSocket server = new ServerSocket(Utility.JobTrackerPort);
			while (taskTrackerComms.size() < Utility.TASKTRACKERS.size()) {
				Socket socket = server.accept();
				Communication communication = new Communication(socket);
				Message msg = communication.readMessage();
				if (msg.getMsgType() == Utility.TASKTRACKERREG) {
					System.out.println("Received registration request from node [" + 
								socket.getInetAddress() + ":" + socket.getPort() + "].");
					msg = new Message(Utility.REGACK, taskTrackerIDPool++);
					communication.sendMessage(msg);
					taskTrackerComms.add(communication);
				}
			}
			System.out.println("Received all registration.");
			monitorChannel(server);
			
			/* Prepare job context */
			Configuration conf = new Configuration();
			JobID jobID = new JobID(0, 0);
			JobContext jobContext = new JobContext(conf, jobID);
			jobContexts.put(jobID.getID(), jobContext);
			
			/* Prepare mapper tasks */
			List<MapBasicContext> mapTaskList = new ArrayList<MapBasicContext>();

			TaskID taskid1 = new TaskID(jobID, TaskType.MAPPER_T, 0, 0);
			JZFile file1 = new JZFile(JZFile.LocalFileSystem, "input02.txt", 341);
			IInputSplit split1 = new FileSplit(file1, 0, 150);
			MapBasicContext basicContext1 = new MapBasicContext(conf, taskid1, split1);
			mapTaskList.add(basicContext1);

			TaskID taskid2 = new TaskID(jobID, TaskType.MAPPER_T, 1, 0);
			JZFile file2 = new JZFile(JZFile.LocalFileSystem, "input02.txt", 341);
			IInputSplit split2 = new FileSplit(file2, 151, 150);
			MapBasicContext basicContext2 = new MapBasicContext(conf, taskid2, split2);
			mapTaskList.add(basicContext2);

			jobMappers.put(jobID.getID(), mapTaskList);

			/* Prepare reducer tasks */
			List<ReduceBasicContext> reduceTaskList = new ArrayList<ReduceBasicContext>();

			TaskID taskid3 = new TaskID(jobID, TaskType.REDUCER_T, 0, 0);
			ReduceBasicContext basicContext3 = new ReduceBasicContext(conf, taskid3);
			reduceTaskList.add(basicContext3);
			
			jobReducers.put(jobID.getID(), reduceTaskList);
			
			
			/* Send job context to task trackers */
			System.out.println("New job[" + jobID.getID() + "] added.");
			Message msg = new Message(Utility.NEWJOB, jobContext);
			for (int i = 0; i < taskTrackerComms.size(); i++) {
				taskTrackerComms.get(i).sendMessage(msg);
				Message retMsg = taskTrackerComms.get(i).readMessage();
				if (retMsg.getMsgType() == Utility.NEWJOBACK) {
					//nothing
				}
			}
			
			/* Command the task trackers to run mappers */
			System.out.println("Now run mappers for job[" + jobID.getID() + "].");
			msg = new Message(Utility.RUNMAPPER, mapTaskList);
			for (int i = 0; i < taskTrackerComms.size(); i++) {
				taskTrackerComms.get(i).sendMessage(msg);
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void monitorChannel(final ServerSocket server) {
		new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					while (isRunning) {

						Socket socket = server.accept();
						Communication comm = new Communication(socket);
						Message msg = comm.readMessage();
						
						if (msg.getMsgType() == Utility.MAPPERDONE) {
							System.out.println("Received mappers done message from task tracker[" + 
									msg.getTaskTrackerID() + "] for job[" + msg.getJobID());
							System.out.println("Now run reducers for job[" + msg.getJobID() + "].");
							List<ReduceBasicContext> reduceTaskList = 
									jobReducers.get(msg.getJobID());
							msg = new Message(Utility.RUNREDUCER, reduceTaskList);
							taskTrackerComms.get(msg.getTaskTrackerID()).sendMessage(msg);
						}
						
						else if (msg.getMsgType() == Utility.REDUCERDONE) {
							// nothing, may be close the application
						}
						
						else if (msg.getMsgType() == Utility.TASKTRACKERHEARTBEAT) {
							
						}
					}
					server.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}).start();
	}
}
