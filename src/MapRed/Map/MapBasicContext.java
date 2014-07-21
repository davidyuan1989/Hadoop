package MapRed.Map;

import Conf.Configuration;
import MapRed.Input.IInputSplit;
import MapRed.Task.TaskContext;
import MapRed.Task.TaskID;

public class MapBasicContext extends TaskContext{

	private static final long serialVersionUID = 5L;
	private IInputSplit split;
	
	public MapBasicContext(Configuration conf, TaskID taskid, IInputSplit split) {
		super(conf, taskid);
		this.split = split;
	}
	
	public IInputSplit getInputSplit() {
		return split;
	}

}
