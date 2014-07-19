package JZFS;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;

import Utility.Utility;

public class JZFS {

	private static JZFSNode node;

	public static void main(String[] args) {

		/* Configure the runtime parameters */
		Utility.configure();
		
		/* Parse input arguments */
		parseArgs(args);
		
		/* Monitor user input */
		userInputCommand();
		
		/* Start the file system node */
		node.start();
	}

	private static void userInputCommand() {
		new Thread(new Runnable() {

			@Override
			public void run() {
				System.out.println("Please input command. Supported commands:\n	1. close\n");
				BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
				while (true) {
					try {
						String command = stdin.readLine();
						if ("close".equalsIgnoreCase(command)) {
							node.terminate();
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}).start();
	}

	private static void parseArgs(String[] args) {
		
		try {
			boolean isNameNode = false;
			InetAddress address = InetAddress.getLocalHost();
			int port = 15332;

			for (int i = 0; i < args.length; i++) {

				/* It is the namenode */
				if ("-n".equalsIgnoreCase(args[i])) {
					isNameNode = true;
				}

				/* Address of this node */
				else if ("-a".equalsIgnoreCase(args[i])) {
					if (args.length > i+1) {
						address = InetAddress.getByName(args[i+1]);
					}
				}

				/* Port of this node */
				else if ("-p".equalsIgnoreCase(args[i])) {
					if (args.length > i+1) {
						port = Integer.parseInt(args[i+1]);
					}
				}
			}

			/* Initialize this node */
			if (isNameNode) {
				node = new NameNode(address, port);
			}
			else {
				node = new DataNode(address, port);
			}
			
			System.out.println("Host [ip:port]: [" + address + ":" + port + "]");
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
}
