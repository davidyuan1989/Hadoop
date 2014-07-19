/*
 * Class Name: Utility
 * 		- Helper functions and macros 
 * Author: Jianan Lu (jiananl) & Zizhou Deng (zdeng)
 * 
 */

package Utility;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class Utility {
	
	public static void configure() {
		try {
			NAMENODE = new Machine(InetAddress.getByName("127.0.0.1"), 33150);
			
			DATANODES = new ArrayList<Machine>();
			Machine machine1 = new Machine(InetAddress.getByName("127.0.0.1"), 33160);
			Machine machine2 = new Machine(InetAddress.getByName("127.0.0.1"), 33170);
			DATANODES.add(machine1);
			DATANODES.add(machine2);
			
			JZFSroot = "JZFS/";
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
	/* Normal message types */
	public static final int FILELOCATION = 2;
	public static final int READFILE = 3;
	public static final int DATAREAD = 4;
	public static final int CLOSEFILE = 5;
	public static final int WRITEFILE = 6;
	public static final int DATANODEHEARTBEAT = 7;
	public static final int ACK = 999;	
	public static final int CLOSE = 1000;
	
	/* Configuration Constants */
	public static Machine NAMENODE;
	public static List<Machine> DATANODES;
	public static String JZFSroot = "JZFS/";
	public static String LocalFSroot = "Local/";
	public static String LocalFStemp = "Local/Temp/";
	
	/* Error message types */
	public static final int NOSUCHFILE = 1;
	
}
