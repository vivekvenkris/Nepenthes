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

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteWatchdog;

public class Nepenthes{
	static boolean simulate = true;
	static String fileNameSuffix;
	static String locationPrefix = "/Users/vkrishnan/Desktop/dustbin/";
	static String utcPrefix = "utc: ";
	static String end= "#end";
	static Integer server;
	static Integer basePort = 23000;
	static Integer MAX_PEASOUP_JOBS = 3;

	static LinkedList<String> utcsToSearch = new LinkedList<>();
	static Map<String, MyExecuteResultHandler> runningSMIRFSoups = new HashMap<>();
	static private boolean shutdown = false;

	static Thread startThread = new Thread(new Runnable() {

		@Override
		public void run() {
			while(!shutdown){
				synchronized (utcsToSearch) {
					for(Iterator<String> iterator = utcsToSearch.iterator(); iterator.hasNext();){
						MyExecuteResultHandler resultHandler = null;
						try{
							if(runningSMIRFSoups.size() > 3 ){
								// log overload error
								continue;
							}
							String utc = iterator.next();
							if(runningSMIRFSoups.get(utc)!=null) {
								// log duplicate as error
								iterator.remove();
								continue;
							}

							resultHandler = startSMIRFSoupForUTC(utc);
							synchronized (runningSMIRFSoups) {
								runningSMIRFSoups.put(utc, resultHandler);
							}
							iterator.remove();
						} catch (Exception e) {
							e.printStackTrace();
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
	
	static Thread monitorThread = new Thread( new Runnable() {

		@Override
		public void run() {
			while(!shutdown){
				synchronized (runningSMIRFSoups) {
					Set<Entry<String, MyExecuteResultHandler>> entrySet = runningSMIRFSoups.entrySet();

					for(Iterator<Entry<String, MyExecuteResultHandler>> iterator = entrySet.iterator(); iterator.hasNext();){
						Entry<String, MyExecuteResultHandler> entry = iterator.next();
						MyExecuteResultHandler resultHandler = entry.getValue();
						if(resultHandler.isComplete()) iterator.remove();
					}



				}
			}

		}
	});

	
	

	public static void main(String[] args) throws UnknownHostException {

		// Not a simulation if host contains "obs.molonglo.local"
		simulate = simulate();
		
		// get host name and get the server number. Assumes the only numbers in the host name is the server number. 
		// eg: mpsr-bf08.obs.molonglo.local returns 08 as the server name.
		String hostname = (simulate && args.length > 0) ? args[0] : Inet4Address.getLocalHost().getHostName();
		server = Integer.parseInt(hostname.replaceAll("\\D+", ""));
		
		fileNameSuffix = ".bf0"+server+".pts";
		Integer port = basePort + (simulate ? server : 0);

		startThread.start();
		monitorThread.start();
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

						String utc = input.replaceAll(utcPrefix, "");

						String utcDir = locationPrefix+utc;
						File dir = new File(utcDir);
						if(!dir.exists()) dir.mkdir();

						input = in.readLine();
						Integer obsID = Integer.parseInt(input);
						// ADD TO DATABASE THAT PEASOUP STARTED

						String pointsFile = String.format("bf%02d.uniq.points", server);
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

	public static void addShutDownHook(){
		Runtime.getRuntime().addShutdownHook(new Thread( new Runnable() {

			@Override
			public void run() {
				System.err.println("Preparing shutdown...");
				shutdown = true;
				while(startThread.isAlive() || monitorThread.isAlive());
			}
		}));
	}


	public static MyExecuteResultHandler startSMIRFSoupForUTC(String utc) throws IOException{

		/* SMIRFSoup <OBS DIR> <NFB> <uniq.pts file> */
		CommandLine commandLine = new CommandLine("SMIRFSoup");
		commandLine.addArgument(locationPrefix+utc);
		commandLine.addArgument("360");
		commandLine.addArgument(utc+".bf0"+server+".pts");

		ExecuteWatchdog watchDog = new ExecuteWatchdog(4 * 15 * 60 * 1000);

		DefaultExecutor executor = new DefaultExecutor();
		executor.setWatchdog(watchDog);

		MyExecuteResultHandler resultHandler =  new MyExecuteResultHandler();

		executor.execute(commandLine, resultHandler);

		return resultHandler;
	}



	public static boolean simulate(){
		try{
			return !InetAddress.getLocalHost().getHostName().contains("obs.molonglo.local");
		}catch (Exception e) {
			e.printStackTrace();
			return false;
		}

	}

	public static Map<String,Integer> populateNepenthesServers(){

		Map<String, Integer> nepenthesServers = new HashMap<>();
		nepenthesServers.put("mpsr-bf00.obs.molonglo.local", 23000);
		nepenthesServers.put("mpsr-bf01.obs.molonglo.local", 23001);
		nepenthesServers.put("mpsr-bf02.obs.molonglo.local", 23002);
		nepenthesServers.put("mpsr-bf03.obs.molonglo.local", 23003);
		nepenthesServers.put("mpsr-bf04.obs.molonglo.local", 23004);
		nepenthesServers.put("mpsr-bf05.obs.molonglo.local", 23005);
		nepenthesServers.put("mpsr-bf06.obs.molonglo.local", 23006);
		nepenthesServers.put("mpsr-bf07.obs.molonglo.local", 23007);
		nepenthesServers.put("mpsr-bf08.obs.molonglo.local", 23008);
		return Collections.unmodifiableMap(nepenthesServers);
	}
}
