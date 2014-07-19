package mapred.org.apache.hadoop.mapreduce;

import java.io.IOException;

public class Mapper {
	public class Context {
	}

	/**
	 * Called once at the start of the task.
	 */
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// NOTHING
	}
}
