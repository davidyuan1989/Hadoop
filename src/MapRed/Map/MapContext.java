package MapRed.Map;

import java.io.IOException;

import Conf.Configuration;
import MapRed.IStatusReporter;
import MapRed.Input.IInputSplit;
import MapRed.Input.IRecordReader;
import MapRed.Output.IRecordWriter;
import MapRed.Output.OutputCommitter;
import MapRed.Task.TaskID;

public class MapContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT> extends 
				MapBasicContext {

	private static final long serialVersionUID = 1L;
	
	public IRecordWriter<KEYOUT,VALUEOUT> writer;
	public IStatusReporter reporter;
	public OutputCommitter committer;
	public IRecordReader<KEYIN,VALUEIN> reader;

	public MapContext(Configuration conf, TaskID taskid,
			IRecordReader<KEYIN,VALUEIN> reader,
			IRecordWriter<KEYOUT,VALUEOUT> writer,
			OutputCommitter committer,
			IStatusReporter reporter,
			IInputSplit split) {
		super(conf, taskid, split);
		this.writer = writer;
		this.reader = reader;
		this.committer = committer;
		this.reporter = reporter;
	}

	public KEYIN getCurrentKey() throws IOException, InterruptedException {
		return reader.getCurrentKey();
	}

	public VALUEIN getCurrentValue() throws IOException, InterruptedException {
		return reader.getCurrentValue();
	}

	public boolean nextKeyValue() throws IOException, InterruptedException {
		return reader.nextKeyValue();
	}
	
	public void write(KEYOUT key, VALUEOUT value
			) throws IOException, InterruptedException {
		writer.write(key, value);
	}
	
	public void close() {
		try {
			if (reader != null) {
				reader.close();
			}
			if (writer != null) {
				writer.close(null);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
