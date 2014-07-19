package JZFS;

import java.net.InetAddress;

import Utility.Machine;

public abstract class JZFSNode extends Machine {

	private static final long serialVersionUID = 3L;
	protected boolean isRunning = true;
	
	public JZFSNode(InetAddress ipAddress, int port) {
		super(ipAddress, port);
	}

	public void terminate() {
		isRunning = false;
	}
	
	public abstract void start();
}
