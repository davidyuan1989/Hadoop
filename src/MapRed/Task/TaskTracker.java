package MapRed.Task;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import Conf.Configuration;
import MapRed.Input.IInputFormat;
import MapRed.Input.IInputSplit;
import MapRed.Input.IRecordReader;
import MapRed.Job.JobContext;
import MapRed.Map.MapBasicContext;
import MapRed.Map.Mapper;
import MapRed.Output.IOutputFormat;
import MapRed.Output.IRecordWriter;
import MapRed.Output.SequenceFileRecordSortedWriter;
import MapRed.Partition.HashPartitioner;
import MapRed.Reduce.ReduceBasicContext;
import MapRed.Reduce.Reducer;
import MapRed.Task.TaskContext.TaskStatus;
import Utility.Communication;
import Utility.JZFile;
import Utility.JZSequenceFile;
import Utility.Message;
import Utility.Utility;

@SuppressWarnings("rawtypes")
public class TaskTracker {

	private static Communication jobTrackerComm;
	private static int taskTrackerID = 0;
	private static boolean isRunning = true;

	private static Map<String, JobContext> jobContexts = 
			new HashMap<String, JobContext>();
	private static Map<String, List<Mapper.Context>> jobMappers = 
			new HashMap<String, List<Mapper.Context>>();
	private static Map<String, List<Reducer.Context>> jobReducers = 
			new HashMap<String, List<Reducer.Context>>();

	private static int numMappers;
	private static int numReducers;

	public static void main(String[] args) {
		/* Set up configuration */
		Utility.configure();
		jobTrackerComm = 
				new Communication(Utility.JOBTRACKER.ipAddress, Utility.JOBTRACKER.port);

		/* Register this task tracker */
		System.out.println("Registering on job tracker...");
		Message msg = new Message(Utility.TASKTRACKERREG);
		jobTrackerComm.sendMessage(msg);

		msg = jobTrackerComm.readMessage();
		if (msg.getMsgType() == Utility.REGACK) {
			taskTrackerID = msg.getTaskTrackerID();
			System.out.println("Successfully registered.");
		}

		/* Wait for incoming commands */
		while (isRunning) {

			msg = jobTrackerComm.readMessage();

			if (msg.getMsgType() == Utility.NEWJOB) {
				JobContext jobContext = msg.getJobContext();
				String jobID = jobContext.getJobID().getID();
				System.out.println("Receiced new job from job[" + jobID + "] tracker");
				if (!jobContexts.containsKey(jobID)) {
					jobContexts.put(jobID, jobContext);
				}
				msg = new Message(Utility.NEWJOBACK);
				jobTrackerComm.sendMessage(msg);
			}

			else if (msg.getMsgType() == Utility.RUNMAPPER) {
				System.out.println("Received RUNMAPPER command from job tracker.");
				List<MapBasicContext> mapBasicContexts = msg.getMapContexts();
				if (mapBasicContexts.size() != 0) {
					String jobID = mapBasicContexts.get(0).getJobID().getID();
					JobContext jobContext = jobContexts.get(jobID);
					numMappers = mapBasicContexts.size();
					launchMappers(jobContext, mapBasicContexts);
				}
			}

			else if (msg.getMsgType() == Utility.RUNREDUCER) {
				System.out.println("Received RUNREDUCER command from job tracker.");
				List<ReduceBasicContext> reduceBasicContexts = msg.getReduceContexts();
				if (reduceBasicContexts.size() != 0) {
					String jobID = reduceBasicContexts.get(0).getJobID().getID();
					JobContext jobContext = jobContexts.get(jobID);
					numReducers = reduceBasicContexts.size();
					launchReducers(jobContext, reduceBasicContexts);
				}
			}

			else if (msg.getMsgType() == Utility.CLOSE) {
				isRunning = false;
			}
		}

		jobTrackerComm.close();
	}



	@SuppressWarnings("unchecked")
	private static void launchMappers(JobContext jobContext, List<MapBasicContext> taskList) {
		try {
			if (taskList == null) return;

			final String jobID = jobContext.getJobID().getID();
			Configuration conf = jobContext.getConfiguration();
			Comparator comparator = conf.getComparator();

			Class mapperClass = conf.getMapperClass();
			Class inputFormatClass = conf.getInputFormatClass();

			Constructor inputFormatConst = inputFormatClass.getConstructor(new Class[]{});
			IInputFormat<Object, Object> inputFormat = 
					(IInputFormat<Object, Object>) inputFormatConst.newInstance(new Object[] {});
			Constructor mapperConst = mapperClass.getConstructor(new Class[]{});

			for (int i = 0; i < taskList.size(); i++) {

				MapBasicContext basicMap = taskList.get(i);

				IInputSplit split = basicMap.getInputSplit();
				IRecordReader<Object, Object> reader = inputFormat.createIRecordReader(split, null);

				String fileName = Utility.MapperOutputNameBase + 
						String.valueOf(basicMap.getTaskID().getTaskID()) + Utility.MiddleOutputFileSuffix;
				JZSequenceFile outputFile = new JZSequenceFile(JZFile.LocalFileSystem, fileName, 0, 0);
				IRecordWriter<Object, Object> writer = 
						new SequenceFileRecordSortedWriter<Object, Object>(outputFile, comparator, basicMap.getTaskID().getID());

				final Mapper<Object, Object, Object, Object> mapper = 
						(Mapper<Object, Object, Object, Object>) mapperConst.newInstance(new Object[]{});

				final Mapper<Object, Object, Object, Object>.Context context = 
						mapper.new Context(conf, basicMap.getTaskID(), reader, writer, null, null, split);

				if (jobMappers.containsKey(jobID)) {
					List<Mapper.Context> mapContexts = jobMappers.get(jobID);
					mapContexts.add(context);
				}
				else {
					List<Mapper.Context> mapContexts = new ArrayList<Mapper.Context>();
					mapContexts.add(context);
					jobMappers.put(jobID, mapContexts);
				}

				new Thread(new Runnable() {

					@Override
					public void run() {
						try {
							context.setTaskStatus(TaskStatus.INPROGRESS);
							mapper.run(context);
							updateMapperStatus(context);
						} catch (IOException | InterruptedException e) {
							e.printStackTrace();
						}
					}
				}).start();
			}

		} catch (NoSuchMethodException | SecurityException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	private static void partition(JobContext jobContext) {

		String jobID = jobContext.getJobID().getID();
		Configuration conf = jobContext.getConfiguration();
		Comparator comparator = conf.getComparator();
		int numReducers = conf.getNumReducers() * Utility.TASKTRACKERS.size();

		HashPartitioner<Object, List<Object>> partitioner = 
				new HashPartitioner<Object, List<Object>>();

		List<String> files = new ArrayList<String>();

		if (jobMappers.containsKey(jobID)) {
			List<Mapper.Context> mapContexts = jobMappers.get(jobID);
			for (int i = 0; i < mapContexts.size(); i++) {
				String fileName = mapContexts.get(i).writer.getOutputFile().path;
				files.add(fileName);
			}
		}

		String jobTaskTrackerID = jobID + String.valueOf(taskTrackerID);
		partitioner.partition(files, numReducers, comparator, jobTaskTrackerID);
	}

	@SuppressWarnings("unchecked")
	private static void launchReducers(JobContext jobContext, List<ReduceBasicContext> taskList) {
		try {
			if (taskList == null) return;

			String jobID = jobContext.getJobID().getID();
			Configuration conf = jobContext.getConfiguration();
			Comparator comparator = conf.getComparator();

			Class reducerClass = conf.getReducerClass();
			Class outputFormatClass = conf.getOutputFormatClass();

			Constructor outputFormatConst = outputFormatClass.getConstructor(new Class[]{});
			IOutputFormat<Object, Object> outputFormat = 
					(IOutputFormat<Object, Object>) outputFormatConst.newInstance(new Object[] {});
			Constructor reducerConst = reducerClass.getConstructor(new Class[]{});

			for (int i = 0; i < taskList.size(); i++) {

				ReduceBasicContext basicContext = taskList.get(i);
				String taskID = basicContext.getTaskID().getID();
				int reducerID = basicContext.getTaskID().getTaskID();

				/* Get reader */
				/* Get partition files */
				List<String> files = new ArrayList<String>();
				for (int j = 0; j < Utility.TASKTRACKERS.size(); j++) {
					String jobTaskTrackerID = jobID + String.valueOf(j);
					files.add(HashPartitioner.createPartitionFileName(jobTaskTrackerID, reducerID));
				}

				/* Merged partition files */
				String fileName = Utility.LocalFStemp + taskID + "_input.dat";
				JZSequenceFile reduceInputFile = 
						new JZSequenceFile(JZFile.LocalFileSystem, fileName, 0, 0);

				JZSequenceFile.staticMerge(files, reduceInputFile, comparator);

				JZSequenceFile.Reader<Object, List<Object>> reader = 
						new JZSequenceFile.Reader<Object, List<Object>>(reduceInputFile);

				/* Get writer */
				String outputFileName = Utility.OutputFolder + taskID + "_output.dat";
				JZSequenceFile reduceOutputFile = 
						new JZSequenceFile(JZFile.JZFileSystem, outputFileName, 0, 0);
				IRecordWriter<Object, Object> writer = outputFormat.getRecordWriter(reduceOutputFile, basicContext);

				/* Create reducer */
				final Reducer<Object, List<Object>, Object, Object> reducer = 
						(Reducer<Object, List<Object>, Object, Object>) reducerConst.newInstance(new Object[]{});
				final Reducer<Object, List<Object>, Object, Object>.Context context = 
						reducer.new Context(conf, basicContext.getTaskID(), reader, writer, null, null);

				/* Add it to the hash map */
				if (jobReducers.containsKey(jobID)) {
					List<Reducer.Context> reduceContexts = jobReducers.get(jobID);
					reduceContexts.add(context);
				}
				else {
					List<Reducer.Context> reduceContexts = new ArrayList<Reducer.Context>();
					reduceContexts.add(context);
					jobReducers.put(jobID, reduceContexts);
				}

				new Thread(new Runnable() {

					@Override
					public void run() {
						try {
							context.setTaskStatus(TaskStatus.INPROGRESS);
							reducer.run(context);
							updateReducerStatus(context);
						} catch (IOException | InterruptedException e) {
							e.printStackTrace();
						}
					}
				}).start();
			}

		} catch (NoSuchMethodException | SecurityException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private synchronized static void updateMapperStatus(Mapper.Context context) {
		try {
			System.out.println("Mapper[" + context.getTaskID().getID() + "] has finished.");
			context.setTaskStatus(TaskStatus.FINISHED);
			String jobID = context.getJobID().getID();
			if (jobMappers.get(jobID).size() == numMappers) {

				List<Mapper.Context> mapContexts = jobMappers.get(jobID);
				boolean finished = true;
				for (int i = 0; i < mapContexts.size(); i++) {
					if (mapContexts.get(i).getTaskStatus() != TaskStatus.FINISHED) {
						finished = false;
						break;
					}
				}
				if (finished) {
					partition(jobContexts.get(jobID));
					System.out.println("All mappers done and partitioned. ");
					Message msg = new Message(Utility.MAPPERDONE, taskTrackerID, jobID);
					Communication comm = 
							new Communication(Utility.JOBTRACKER.ipAddress, Utility.JOBTRACKER.port);
					comm.sendMessage(msg);
					comm.close();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private synchronized static void updateReducerStatus(Reducer.Context context) {
		String jobID = context.getJobID().getID();
		if (jobReducers.get(jobID).size() == numReducers) {
			try {
				context.setTaskStatus(TaskStatus.FINISHED);
				List<Reducer.Context> reduceContexts = jobReducers.get(jobID);
				boolean finished = true;
				for (int i = 0; i < reduceContexts.size(); i++) {
					if (reduceContexts.get(i).getTaskStatus() != TaskStatus.FINISHED) {
						finished = false;
						break;
					}
				}
				if (finished) {
					System.out.println("All reducers done. ");
					Message msg = new Message(Utility.REDUCERDONE, taskTrackerID, jobID);
					Communication comm = 
							new Communication(Utility.JOBTRACKER.ipAddress, Utility.JOBTRACKER.port);
					comm.sendMessage(msg);
					comm.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
