package MapRed.Map;

import java.io.IOException;

import Conf.Configuration;
import MapRed.IStatusReporter;
import MapRed.Input.IInputSplit;
import MapRed.Input.IRecordReader;
import MapRed.Output.IRecordWriter;
import MapRed.Output.OutputCommitter;
import MapRed.Task.TaskContext;
import MapRed.Task.TaskID;

public class MapContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT> extends 
				TaskContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	private IRecordReader<KEYIN,VALUEIN> reader;
	private IInputSplit split;

	public MapContext(Configuration conf, TaskID taskid,
			IRecordReader<KEYIN,VALUEIN> reader,
			IRecordWriter<KEYOUT,VALUEOUT> writer,
			OutputCommitter committer,
			IStatusReporter reporter,
			IInputSplit split) {
		super(conf, taskid, writer, committer, reporter);
		this.reader = reader;
		this.split = split;
	}

	public IInputSplit getInputSplit() {
		return split;
	}

	@Override
	public KEYIN getCurrentKey() throws IOException, InterruptedException {
		return reader.getCurrentKey();
	}

	@Override
	public VALUEIN getCurrentValue() throws IOException, InterruptedException {
		return reader.getCurrentValue();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return reader.nextKeyValue();
	}
}
