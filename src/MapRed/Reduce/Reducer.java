package MapRed.Reduce;

import java.io.IOException;

import Conf.Configuration;
import MapRed.IStatusReporter;
import MapRed.Output.IRecordWriter;
import MapRed.Output.OutputCommitter;
import MapRed.Task.TaskID;
import Utility.JZSequenceFile;

public abstract class Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {
	public class Context 
	extends ReduceContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {
		
		private static final long serialVersionUID = 1L;

		public Context(Configuration conf, TaskID taskid,
				JZSequenceFile.Reader<KEYIN, VALUEIN> input, 
				IRecordWriter<KEYOUT,VALUEOUT> output,
				OutputCommitter committer,
				IStatusReporter reporter
				) throws IOException, InterruptedException {
			super(conf, taskid, input, output, committer, reporter);
		}
	}

	/**
	 * Called once at the start of the task.
	 */
	 protected void setup(Context context
			 ) throws IOException, InterruptedException {
		 // NOTHING
	 }

	 /**
	  * This method is called once for each key. Most applications will define
	  * their reduce class by overriding this method. The default implementation
	  * is an identity function.
	  */
	 protected void reduce(KEYIN key, VALUEIN values, Context context
			 ) throws IOException, InterruptedException {


	 }

	 /**
	  * Called once at the end of the task.
	  */
	 protected void cleanup(Context context
			 ) throws IOException, InterruptedException {
		 context.close();
	 }

	 /**
	  * Advanced application writers can use the 
	  * {@link #run(org.apache.hadoop.mapreduce.Reducer.Context)} method to
	  * control how the reduce task works.
	  */
	 public void run(Context context) throws IOException, InterruptedException {
		 setup(context);
		 while (context.nextKeyValue()) {
			 reduce(context.getCurrentKey(), context.getCurrentValue(), context);
		 }
		 cleanup(context);
	 }
}
