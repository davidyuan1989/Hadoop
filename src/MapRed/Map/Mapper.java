package MapRed.Map;

import java.io.IOException;

import Conf.Configuration;
import MapRed.IStatusReporter;
import MapRed.Input.IInputSplit;
import MapRed.Input.IRecordReader;
import MapRed.Output.IRecordWriter;
import MapRed.Output.OutputCommitter;
import MapRed.Task.TaskID;

public class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	/* MapContext is a raw type class. This new context class specifies the types */
	public class Context extends MapContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {

		private static final long serialVersionUID = 1L;

		public Context(Configuration conf, TaskID taskid,
				IRecordReader<KEYIN,VALUEIN> reader,
				IRecordWriter<KEYOUT,VALUEOUT> writer,
				OutputCommitter committer,
				IStatusReporter reporter,
				IInputSplit split) throws IOException, InterruptedException {
			super(conf, taskid, reader, writer, committer, reporter, split);
		}
	}

	/* Called at the beginning of the task */
	protected void setup(Context context
			) throws IOException, InterruptedException {
		// NOTHING
	}

	@SuppressWarnings("unchecked")
	protected void map(KEYIN key, VALUEIN value, 
			Context context) throws IOException, InterruptedException {
		context.write((KEYOUT) key, (VALUEOUT) value);
	}

	/* Called before the end of the task */
	protected void cleanup(Context context
			) throws IOException, InterruptedException {
		context.close();
	}

	public void run(Context context) throws IOException, InterruptedException {
		setup(context);
		while (context.nextKeyValue()) {
			map(context.getCurrentKey(), context.getCurrentValue(), context);
		}
		cleanup(context);
	}
}
