/*
 * Class Name: Utility
 * 		- Helper functions and macros 
 * Author: Jianan Lu (jiananl) & Zizhou Deng (zdeng)
 * 
 */

package Utility;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

public class Utility {
	
	public static void configure() {
		try {
			FileInputStream in = new FileInputStream("configuration.txt");
			BufferedReader bufferIn = new BufferedReader(new InputStreamReader(in));
			int beginIndex = 0;
			int endIndex = 0;
			
			String temp = "";
			String tempIP = "";
			int tempPort = 0;
			int tempQuantity = 0;
			int tempInt = 0;
			
			ArrayList<Integer> overallPortList = new ArrayList<Integer>(4);

			//read NameNode IP address
			tempIP = bufferIn.readLine();
			endIndex = tempIP.indexOf(' ', beginIndex);
			tempIP = tempIP.substring(beginIndex, endIndex);
			tempIP.trim();
			
			//read NameNode port
			temp = bufferIn.readLine();
			endIndex = temp.indexOf(' ', beginIndex);
			temp = temp.substring(beginIndex, endIndex);
			temp.trim();
			tempPort = Integer.parseInt(temp);
	
			NAMENODE = new Machine(InetAddress.getByName(tempIP), tempPort);
			overallPortList.add(NAMENODE.port);
			System.out.println("NAMENODE IP is "+ NAMENODE.ipAddress + " port is " + NAMENODE.port);
			
			//read DataNode quantity
			temp = bufferIn.readLine();
			endIndex = temp.indexOf(' ', beginIndex);
			temp = temp.substring(beginIndex, endIndex);
			temp.trim();
			tempQuantity = Integer.parseInt(temp);
			
			System.out.println("DataNode quantity is "+ tempQuantity );
			
			DATANODES = new ArrayList<Machine>();
			ArrayList<Integer> dataNodePortList = new ArrayList<Integer>();

			//read DataNode IPs and ports
			for (int i = 0; i < tempQuantity; i++) {
				
				//read DataNode IP address
				tempIP = bufferIn.readLine();
				endIndex = tempIP.indexOf(' ', beginIndex);
				tempIP = tempIP.substring(beginIndex, endIndex);
				tempIP.trim();
				
				//read DataNode port
				temp = bufferIn.readLine();
				endIndex = temp.indexOf(' ', beginIndex);
				temp = temp.substring(beginIndex, endIndex);
				temp.trim();
				tempPort = Integer.parseInt(temp);
				
				dataNodePortList.add(tempPort);
				
				Machine machine = new Machine(InetAddress.getByName(tempIP), tempPort);
				DATANODES.add(machine);
				
				if(i == 0){
					overallPortList.add(machine.port);
				}
				
				System.out.println("DataNode IP is "+ machine.ipAddress + " port is " + machine.port);

				
			}
			
			tempInt = dataNodePortList.get(0);
			for(int i = 0;i < dataNodePortList.size();i++){
				if (tempInt != dataNodePortList.get(i)) {
					System.out.println("Port number of DataNodes must be the same with each other,"
							+ " please change configuration file.");
					System.exit(1);
				}
			}
			
			
			
			//read master node IPs 
			tempIP = bufferIn.readLine();
			endIndex = tempIP.indexOf(' ', beginIndex);
			tempIP = tempIP.substring(beginIndex, endIndex);
			tempIP.trim();
			
			//read master node port
			temp = bufferIn.readLine();
			endIndex = temp.indexOf(' ', beginIndex);
			temp = temp.substring(beginIndex, endIndex);
			temp.trim();
			tempPort = Integer.parseInt(temp);
			
			JOBTRACKER = new Machine(InetAddress.getByName(tempIP), tempPort);
			overallPortList.add(JOBTRACKER.port);
			
			System.out.println("JOBTRACKER IP is "+ JOBTRACKER.ipAddress + " port is " + JOBTRACKER.port);
			
			//read participant node quantity
			temp = bufferIn.readLine();
			endIndex = temp.indexOf(' ', beginIndex);
			temp = temp.substring(beginIndex, endIndex);
			temp.trim();
			tempQuantity = Integer.parseInt(temp);
			
			System.out.println("participant node quantity is "+ tempQuantity );
			
			TASKTRACKERS = new ArrayList<Machine>();
			
			ArrayList<Integer> participantNodePortList = new ArrayList<Integer>();

			//read TASKTRACKERS IPs and ports
			for (int i = 0; i < tempQuantity; i++) {
				
				//read TASKTRACKER IP address
				tempIP = bufferIn.readLine();
				endIndex = tempIP.indexOf(' ', beginIndex);
				tempIP = tempIP.substring(beginIndex, endIndex);
				tempIP.trim();
				
				//read TASKTRACKER port
				temp = bufferIn.readLine();
				endIndex = temp.indexOf(' ', beginIndex);
				temp = temp.substring(beginIndex, endIndex);
				temp.trim();
				tempPort = Integer.parseInt(temp);
				
				participantNodePortList.add(tempPort);
		
				Machine machine = new Machine(InetAddress.getByName(tempIP), tempPort);
				TASKTRACKERS.add(machine);
				
				if(i == 0){
					overallPortList.add(machine.port);
					
					if(overallPortList.get(0).equals(overallPortList.get(1)) || overallPortList.get(0).equals(overallPortList.get(2)) || overallPortList.get(0).equals(overallPortList.get(3)) ||
							overallPortList.get(1).equals(overallPortList.get(2)) || overallPortList.get(1).equals(overallPortList.get(3)) || overallPortList.get(2).equals(overallPortList.get(3))){
						System.out.println("Port number of master node, participant node, NameNode and DataNode must be different from each other,"
								+ " please change configuration file.");
						System.exit(2);

					}
				}
				System.out.println("TASKTRACKER IP is "+ machine.ipAddress + " port is " + machine.port);

				
			}
			
			tempInt = participantNodePortList.get(0);
			for(int i = 0;i < participantNodePortList.size();i++){
				if (tempInt != participantNodePortList.get(i)) {
					System.out.println("Port number of participant nodes must be the same with each other,"
							+ " please change configuration file.");
					System.exit(1);
				}
			}
			
			bufferIn.close();
		} catch (IOException e) {
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
	
	/* Read only configuration constants */
	public static final String JZFSroot = "JZFS/";
	public static final String JZFStemp = "JZFS/Temp/";
	public static final String LocalFSroot = "Local/";
	public static final String LocalFStemp = "Local/Temp/";
	public static final String MapperOutputNameBase = LocalFStemp + "Mapper";
	public static final String PartitionOutputNameBase = JZFStemp + "Partition";
	public static final String MiddleOutputFileSuffix = ".dat";
	public static final String InputFolder = JZFSroot + "Input/";
	public static final String OutputFolder = JZFSroot + "Output/";
	
	/* User defined configuration constants */
	public static Machine NAMENODE;
	public static List<Machine> DATANODES;
	public static Machine JOBTRACKER;
	public static List<Machine> TASKTRACKERS;
	public static int JobTrackerPort = 33150;
	public static int TaskTrackerPort = 33160;
	
	/* Error message types */
	public static final int NOSUCHFILE = 1;
	
}
