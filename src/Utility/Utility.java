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
			//Machine machine2 = new Machine(InetAddress.getByName("127.0.0.1"), 33170);
			DATANODES.add(machine1);
			//DATANODES.add(machine2);
			
			JOBTRACKER = NAMENODE;
			TASKTRACKERS = DATANODES;
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
	
	public static final int TASKTRACKERREG = 100;
	public static final int REGACK = 101;
	public static final int NEWJOB = 102;
	public static final int NEWJOBACK = 103;
	public static final int RUNMAPPER = 104;
	public static final int MAPPERDONE = 105;
	public static final int RUNREDUCER = 106;
	public static final int REDUCERDONE = 107;
	public static final int TASKTRACKERHEARTBEAT = 200;
	public static final int CONF = 201;
	
	/* User defined configuration constants */
	public static Machine NAMENODE;
	public static List<Machine> DATANODES;
	public static Machine JOBTRACKER;
	public static List<Machine> TASKTRACKERS;
	public static int JobTrackerPort = 33150;
	public static int TaskTrackerPort = 33160;
	
	/* Read only configuration constants */
	public static final String JZFSroot = "JZFS/";
	public static final String JZFStemp = "JZFS/Temp/";
	public static final String LocalFSroot = "Local/";
	public static final String LocalFStemp = "Local/Temp/";
	public static final String MapperOutputNameBase = LocalFStemp + "Mapper";
	public static final String PartitionOutputNameBase = LocalFStemp + "Partition";
	public static final String MiddleOutputFileSuffix = ".dat";
	public static final String FILENAME = "JZFS/Input/input.txt";
	
	/* Error message types */
	public static final int NOSUCHFILE = 1;
	
}
