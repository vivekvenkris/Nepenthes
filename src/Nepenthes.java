import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.ProcessDestroyer;
import org.apache.commons.exec.ShutdownHookProcessDestroyer;
import org.javatuples.Pair;

public class Nepenthes{

	static String archivesBase = ConfigManager.getSmirfMap().get("ARCHIVES_BASE");
	static String smirfBase = ConfigManager.getSmirfMap().get("SMIRF_BASE");

	static boolean simulate = Boolean.parseBoolean( ConfigManager.getSmirfMap().get("SIMULATE"));

	static String utcPrefix = ConfigManager.getSmirfMap().get("NEPENTHES_UTC_PREFIX");
	static String end= ConfigManager.getSmirfMap().get("NEPENTHES_END");

	static Integer basePort = Integer.parseInt(ConfigManager.getMopsrMap().get("SMIRF_NEPENTHES_SERVER"));


	static String fileNameSuffix = ConfigManager.getSmirfMap().get("POINTS_FILE_EXT");

	static Integer server;
	static Integer MAX_SMIRFSOUP_JOBS = 3;

	static LinkedList<String> utcsToSearch = new LinkedList<>();
	static Pair<String, MyExecuteResultHandler> runningSMIRFSoup = null;
	static ProcessDestroyer processDestroyer = new ShutdownHookProcessDestroyer();

	static private boolean shutdown = false;

	
	public static void main(String[] args) throws UnknownHostException, ParseException {
		
		System.err.println("Starting Nepenthes"); 

		org.apache.commons.cli.CommandLine line;
		CommandLineParser parser = new DefaultParser();

		Options options = new Options();
		Option hostOption = new Option("h", "host", true, " pretend as host");

		options.addOption(hostOption);
		line = parser.parse(options, args);

		/***
		 *  get host name and get the server number. Assumes the only numbers in the host name is the server number. 
		 *  eg: mpsr-bf08.obs.molonglo.local returns 08 as the server name.
		 */

		String hostname = Inet4Address.getLocalHost().getHostName();

		if(simulate  && hasOption(line, hostOption)) hostname = getValue(line, hostOption);

		if(hostname.contains(".")) hostname = hostname.substring(0,hostname.indexOf("."));

		server = Integer.parseInt(hostname.replaceAll("\\D+", ""));
		Integer port = basePort + server;

		fileNameSuffix = hostname + fileNameSuffix;

		/***
		 * start the SMIRFsouping thread.
		 * Also add the shutdown hook to gracefully end processes.
		 */
		SMIRFSoupManager.start();
		addShutDownHook();



		ServerSocket listener = null;
		try {

			listener = new ServerSocket(port);
			while (true) {

				Socket socket = listener.accept();
				try {

					PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
					BufferedReader in = new BufferedReader( new InputStreamReader(socket.getInputStream()));
					String input = in.readLine();

					if(input.contains(utcPrefix)){

						String utc = input.replaceAll(utcPrefix, "").trim();

						String utcDir = archivesBase+utc;
						File dir = new File(utcDir);
						if(!dir.exists()) dir.mkdir();

						// ADD TO DATABASE THAT PEASOUP STARTED


						String pointsFile = String.format(utc+fileNameSuffix);

						BufferedWriter bw = new BufferedWriter(new FileWriter(new File(utcDir, pointsFile)));
						System.err.println(" writing points file: " + pointsFile);

						while((input = in.readLine()) != null && !input.contains(end)){
							bw.write(input);
							bw.newLine();
							bw.flush();
						}

						if(! socket.isClosed())  out.println("received");
						out.close();
						in.close();
						bw.close();
						synchronized (utcsToSearch) {
							utcsToSearch.add(utc);
						}
					}
				} finally {
					socket.close();
				}
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
		finally {
			try {
				listener.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	static Thread SMIRFSoupManager = new Thread(new Runnable() {

		@Override
		public void run() {
			while(!shutdown){
				if(runningSMIRFSoup == null || runningSMIRFSoup.getValue1().isComplete()) {

					synchronized (utcsToSearch) {

						if(runningSMIRFSoup != null) utcsToSearch.remove(runningSMIRFSoup.getValue0());

						if( utcsToSearch.size() > 0 ) {
							
							String utc = utcsToSearch.get(0);
							MyExecuteResultHandler resultHandler = null;
							
							try {
								resultHandler = startSMIRFSoupForUTC(utc);
							} catch (IOException e) {
								e.printStackTrace();
							}
							runningSMIRFSoup = new Pair<String, MyExecuteResultHandler>(utc, resultHandler);
						}
					}
				}
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	});


	
	public static void addShutDownHook(){
		Runtime.getRuntime().addShutdownHook(new Thread( new Runnable() {

			@Override
			public void run() {
				System.err.println("Preparing shutdown...");
				shutdown = true;
				while(SMIRFSoupManager.isAlive());
			}
		}));
	}


	public static MyExecuteResultHandler startSMIRFSoupForUTC(String utc) throws IOException{

		CommandLine commandLine = new CommandLine("SMIRFSoup");
		commandLine.addArgument("-i");
		commandLine.addArgument(archivesBase+utc);
		

		ExecuteWatchdog watchDog = new ExecuteWatchdog(4 * 15 * 60 * 1000);

		DefaultExecutor executor = new DefaultExecutor();
		executor.setWatchdog(watchDog);
		
		executor.setProcessDestroyer(processDestroyer);

		MyExecuteResultHandler resultHandler =  new MyExecuteResultHandler();

		executor.execute(commandLine, resultHandler);

		return resultHandler;
	}

	public static void help(Options options){
		HelpFormatter formater = new HelpFormatter();
		formater.printHelp("Main", options);
	}

	public static String getValue(org.apache.commons.cli.CommandLine line, Option option){
		return line.getOptionValue(option.getOpt());
	}

	public static boolean hasOption(org.apache.commons.cli.CommandLine line, Option option){
		return line.hasOption(option.getOpt());
	}
}
