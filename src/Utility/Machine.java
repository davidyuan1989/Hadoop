package Utility;

import java.io.Serializable;
import java.net.InetAddress;

public class Machine implements Serializable {
	
	private static final long serialVersionUID = 2L;
	public InetAddress ipAddress;
	public int port;
	public String id;
	
	public Machine(InetAddress ipAddress, int port) {
		this.ipAddress = ipAddress;
		this.port = port;
		this.id = "[" + ipAddress.toString() + ":" + String.valueOf(port) + "]";
	}
}
