package JZFS;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import Utility.Communication;
import Utility.Machine;
import Utility.Message;
import Utility.Utility;

public class NameNode extends JZFSNode {

	private static final long serialVersionUID = 4L;
	private Map<String, DataNode> files = new HashMap<String, DataNode>();
	private Map<String, DataNode> dataNodes = new HashMap<String, DataNode>();
	
	public NameNode(InetAddress ipAddress, int port) {
		super(ipAddress, port);
		
		/* For test */
		DataNode dataNode1 = new DataNode(Utility.DATANODES.get(0).ipAddress, Utility.DATANODES.get(0).port);
		DataNode dataNode2 = new DataNode(Utility.DATANODES.get(1).ipAddress, Utility.DATANODES.get(1).port);
		dataNodes.put(dataNode1.id, dataNode1);
		dataNodes.put(dataNode2.id, dataNode2);
		//files.put("JZFS/input/input01.txt", dataNode1);
		//files.put("JZFS/input/input02.txt", dataNode2);
	}

	@Override
	public void start() {
		
		try {
			ServerSocket server = new ServerSocket(Utility.NAMENODE.port);

			while (isRunning) {
				final Socket socket = server.accept();
				new Thread(new Runnable() {

					@Override
					public void run() {
						Communication comm = new Communication(socket);
						nameNodeHandler(comm);
					}
				}).start();
			}

			server.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void nameNodeHandler(Communication comm) {

		Message msg = comm.readMessage();
		
		if (msg.getMsgType() == Utility.READFILE) {
			
			/* Search the file in the hash table */
			DataNode dataNode = files.get(msg.getFileName());
			
			if (dataNode == null) {
				msg = new Message(Utility.NOSUCHFILE);
			}
			else {
				System.out.println("File " + msg.getFileName() + " is stored in node [" 
									+ dataNode.ipAddress + ":" + dataNode.port + "]");
				msg = new Message(Utility.FILELOCATION, new Machine(dataNode.ipAddress, dataNode.port));
			}
			
			comm.sendMessage(msg);
			comm.close();
		}

		else if (msg.getMsgType() == Utility.WRITEFILE) {
			
			/* Search the file in the hash table */
			DataNode dataNode = files.get(msg.getFileName());
			
			/* There is no such file, then pick one data node to create a new file */
			if (dataNode == null) {
				dataNode = loadBalancer();
				System.out.println("\nData node [" + dataNode.ipAddress + ":" + dataNode.port
								+ "]" + " is picked to store file " + msg.getFileName() + "\n");
			}
			
			/* Update the files table */
			files.put(msg.getFileName(), dataNode);
			
			/* Reply the address of the picked data node */
			msg = new Message(Utility.FILELOCATION, new Machine(dataNode.ipAddress, dataNode.port));
			comm.sendMessage(msg);
			comm.close();
		}
		
		else if (msg.getMsgType() == Utility.DATANODEHEARTBEAT) {
			if (dataNodes.containsKey(msg.getNodeID())) {
				dataNodes.get(msg.getNodeID()).spaceUsage = msg.getDataNodeSpaceUsed();
				//System.out.println("Node " + msg.getNodeID() + " space usage updated.");
			}
		}
	}
	
	private DataNode loadBalancer() {
		
		if (dataNodes.size() <= 0) {
			System.out.println("Error! There is no data node!");
			return null;
		}
		
		DataNode dataNode = null;
		long size = Long.MAX_VALUE;
		
		/* Traverse all the data nodes to get the node with the least files */
		Iterator<Entry<String, DataNode>> iterator = dataNodes.entrySet().iterator();
		while (iterator.hasNext()) {
			Entry<String, DataNode> entry = iterator.next();
			DataNode node = entry.getValue();
			long nodeSize = node.spaceUsage;
			
			System.out.println("Current space used for node [" + node.ipAddress + ":" 
								+ node.port + "]: " + nodeSize);
			
			if (nodeSize <= size) {
				size = nodeSize;
				dataNode = node;
			}
		}
		
		return dataNode;
	}
}
