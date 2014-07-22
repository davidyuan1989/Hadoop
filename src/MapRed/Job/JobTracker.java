package MapRed.Job;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.File;

import Conf.Configuration;
import JZFS.JZFSFileOutputStream;
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

	private static enum TaskTrackerJobStatus {
		INIT, MAPPING, REDUCING, FINISHED
	};

	private static class TaskTrackerContext {
		@SuppressWarnings("unused")
		public Map<String, TaskTrackerJobStatus> jobStatuses = new HashMap<String, TaskTrackerJobStatus>();
		public Map<String, List<MapBasicContext>> jobMappers = new HashMap<String, List<MapBasicContext>>();
		public Map<String, List<ReduceBasicContext>> jobReducers = new HashMap<String, List<ReduceBasicContext>>();
	}

	private static int curJob = 0;
	private final static int jobTrackID = 1;
	private static Map<String, Integer> completedMapNodes = new HashMap<String, Integer>();
	private static Map<String, Communication> comms = new HashMap<String, Communication>();

	private static Map<Integer, TaskTrackerContext> taskTrackerContexts = new HashMap<Integer, TaskTrackerContext>();

	private static int taskTrackerIDPool = 0;
	private static boolean isRunning = true;

	private static Map<String, JobContext> jobContexts = new HashMap<String, JobContext>();

	private static List<Communication> taskTrackerComms = new ArrayList<Communication>();

	public static void submitJob(Configuration conf, Communication comm) {

		/* Prepare job context */
		JobID jobID = generateJobID();
		comms.put(jobID.getID(), comm);
		completedMapNodes.put(jobID.getID(), 0);
		
		JobContext jobContext = new JobContext(conf, jobID);
		jobContexts.put(jobID.getID(), jobContext);

		generateTrackerContext(conf, jobID);

		/* Send job context to task trackers */
		System.out.println("New job[" + jobID.getID() + "] added.");
		Message msg = new Message(Utility.NEWJOB, jobContext);
		for (int i = 0; i < taskTrackerComms.size(); i++) {
			taskTrackerComms.get(i).sendMessage(msg);
			Message retMsg = taskTrackerComms.get(i).readMessage();
			if (retMsg.getMsgType() == Utility.NEWJOBACK) {
				// nothing
			}
		}

		/* Command the task trackers to run mappers */
		System.out.println("Now run mappers for job[" + jobID.getID() + "].");
		for (int i = 0; i < taskTrackerComms.size(); i++) {
			msg = new Message(Utility.RUNMAPPER,
					taskTrackerContexts.get(i).jobMappers.get(jobID.getID()));
			taskTrackerComms.get(i).sendMessage(msg);
		}
	}
	
	private static String putInputFile(String inputFileName) {
		try {
			
			String outputFileName = Utility.InputFolder + inputFileName;
			byte[] data = new byte[1024];
			int bytes = 0;
			
			FileInputStream inStream = new FileInputStream(new File(inputFileName));
			JZFSFileOutputStream outputStream = new JZFSFileOutputStream(outputFileName);
			
			while ((bytes = inStream.read(data)) != -1) {
				outputStream.write(data, 0, bytes);
			}
			
			inStream.close();
			outputStream.close();
			
			System.out.println("Finish adding input file.");
			return outputFileName;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	private static void generateTrackerContext(Configuration conf, JobID jobID) {
		
		int totalNumMappers = taskTrackerComms.size() * conf.getNumMapper();
		int offsetDelta = 0;
		
		for (int i = 0; i < taskTrackerComms.size(); i++) {
			TaskTrackerContext context = taskTrackerContexts.get(i);

			List<MapBasicContext> mapTaskList = new ArrayList<MapBasicContext>();
			List<ReduceBasicContext> reduceTaskList = new ArrayList<ReduceBasicContext>();

			String inputFile = conf.getInputFileName();
			File file = new File(inputFile);
			
			String FSInputFile = putInputFile(inputFile);
			JZFile fileJZ = new JZFile(JZFile.JZFileSystem,
					FSInputFile, file.length());
			int delta = (int) (file.length() / totalNumMappers);

			for (int j = 0; j < conf.getNumMapper(); j++) {
				TaskID taskid = new TaskID(jobID, TaskType.MAPPER_T, j, 0);
				IInputSplit split;
				if ((j == conf.getNumMapper() - 1) && (i == taskTrackerComms.size() - 1)) {
					split = new FileSplit(fileJZ, offsetDelta, file.length() - offsetDelta);
				} else {
					split = new FileSplit(fileJZ, offsetDelta, delta);
				}
				MapBasicContext basicContext = new MapBasicContext(conf,
						taskid, split);
				mapTaskList.add(basicContext);
				offsetDelta += delta;
			}
			context.jobMappers.put(jobID.getID(), mapTaskList);

			for (int j = 0; j < conf.getNumReducers(); j++) {
				TaskID taskid = new TaskID(jobID, TaskType.REDUCER_T, j, 0);
				ReduceBasicContext basicContext = new ReduceBasicContext(conf,
						taskid);
				reduceTaskList.add(basicContext);
			}
			context.jobReducers.put(jobID.getID(), reduceTaskList);
		}

	}

	private static JobID generateJobID() {
		return new JobID(jobTrackID, ++curJob);
	}

	private static void handleRegister(Message msg, ServerSocket server,
			Communication comm) {

		taskTrackerContexts.put(taskTrackerIDPool, new TaskTrackerContext());

		msg = new Message(Utility.REGACK, taskTrackerIDPool++);
		comm.sendMessage(msg);
		taskTrackerComms.add(comm);
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
						Communication clientCmm;
						switch (msg.getMsgType()) {
						case Utility.MAPPERDONE:
							System.out
									.println("Received mappers done message from task tracker["
											+ msg.getTaskTrackerID()
											+ "] for job[" + msg.getJobID() + "].");
							int maped = completedMapNodes.get(msg.getJobID());
							completedMapNodes.put(msg.getJobID(), ++maped);
							clientCmm = comms.get(msg.getJobID());
							clientCmm.sendMessage(msg);
							if(maped == Utility.TASKTRACKERS.size()){
								String jobID = msg.getJobID();
								System.out.println("Now run reducers for job["
										+ jobID + "].");
								for (int i = 0; i < taskTrackerComms.size(); i++) {
									List<ReduceBasicContext> reduceTaskList = taskTrackerContexts
											.get(i).jobReducers.get(jobID);
									msg = new Message(Utility.RUNREDUCER,
											reduceTaskList);
									taskTrackerComms.get(i).sendMessage(msg);
								}
							}
							break;
						case Utility.CONF:
							System.out.println("Received a new Job");
							submitJob(msg.getConfiguration(), comm);
							break;
						case Utility.REDUCERDONE:
							clientCmm = comms.get(msg.getJobID());
							clientCmm.sendMessage(msg);
							break;
						case Utility.TASKTRACKERHEARTBEAT:
							break;
						default:
							break;
						}
					}
					server.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}).start();
	}

	public static void main(String[] args) {
		Utility.configure();

		/* Receive registration from task trackers */
		ServerSocket server;

		try {
			server = new ServerSocket(Utility.JobTrackerPort);
			while (taskTrackerComms.size() < Utility.TASKTRACKERS.size()) {
				Socket socket = server.accept();
				Communication comm = new Communication(socket);
				Message msg = comm.readMessage();
				if (msg.getMsgType() == Utility.TASKTRACKERREG) {
					System.out
							.println("Received registration request from node ["
									+ socket.getInetAddress()
									+ ":"
									+ socket.getPort() + "].");
					handleRegister(msg, server, comm);
				}
			}
			System.out.println("Received all registration.");
			monitorChannel(server);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
