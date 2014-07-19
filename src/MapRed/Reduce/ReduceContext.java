package MapRed.Reduce;

import java.io.IOException;

import Conf.Configuration;
import MapRed.IStatusReporter;
import MapRed.Output.IRecordWriter;
import MapRed.Output.OutputCommitter;
import MapRed.Task.TaskContext;
import MapRed.Task.TaskID;
import Utility.JZSequenceFile;

public class ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> 
			extends TaskContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT>{

	private JZSequenceFile.Reader<KEYIN, VALUEIN> reader;
	
	public ReduceContext(Configuration conf, TaskID taskid,
            JZSequenceFile.Reader<KEYIN, VALUEIN> reader, 
            IRecordWriter<KEYOUT,VALUEOUT> writer,
            OutputCommitter committer,
            IStatusReporter reporter
            ) throws InterruptedException, IOException{
		super(conf, taskid, writer, committer, reporter);
		this.reader = reader;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return reader.nextKeyValue();
	}

	@Override
	public KEYIN getCurrentKey() throws IOException, InterruptedException {
		return reader.getCurrentKey();
	}

	@Override
	public VALUEIN getCurrentValue() throws IOException, InterruptedException {
		return reader.getCurrentValue();
	}
	
}
