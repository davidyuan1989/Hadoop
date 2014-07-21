package MapRed.Reduce;

import Conf.Configuration;
import MapRed.Task.TaskContext;
import MapRed.Task.TaskID;

public class ReduceBasicContext extends TaskContext{

	private static final long serialVersionUID = 6L;

	public ReduceBasicContext(Configuration conf, TaskID taskid) {
		super(conf, taskid);
	}

}
