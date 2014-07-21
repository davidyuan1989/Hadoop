package MapRed.Reduce;

import java.io.IOException;

import Conf.Configuration;
import MapRed.IStatusReporter;
import MapRed.Output.IRecordWriter;
import MapRed.Output.OutputCommitter;
import MapRed.Task.TaskID;
import Utility.JZSequenceFile;

public class ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> 
			extends ReduceBasicContext {

	private static final long serialVersionUID = 1L;
	
	public IRecordWriter<KEYOUT,VALUEOUT> writer;
	public IStatusReporter reporter;
	public OutputCommitter committer;
	public JZSequenceFile.Reader<KEYIN, VALUEIN> reader;
	
	public ReduceContext(Configuration conf, TaskID taskid,
            JZSequenceFile.Reader<KEYIN, VALUEIN> reader, 
            IRecordWriter<KEYOUT,VALUEOUT> writer,
            OutputCommitter committer,
            IStatusReporter reporter
            ) throws InterruptedException, IOException{
		super(conf, taskid);
		this.reader = reader;
		this.writer = writer;
		this.reporter = reporter;
		this.committer = committer;
	}

	public boolean nextKeyValue() throws IOException, InterruptedException {
		return reader.nextKeyValue();
	}

	public KEYIN getCurrentKey() throws IOException, InterruptedException {
		return reader.getCurrentKey();
	}

	public VALUEIN getCurrentValue() throws IOException, InterruptedException {
		return reader.getCurrentValue();
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
