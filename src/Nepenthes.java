import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
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
import org.apache.commons.exec.Watchdog;
import org.apache.commons.exec.environment.EnvironmentUtils;
import org.javatuples.Pair;


public class Nepenthes{

	static String archivesBase = ConfigManager4Nepenthes.getSmirfMap().get("ARCHIVES_BASE");
	static String smirfBase = ConfigManager4Nepenthes.getSmirfMap().get("SMIRF_BASE");

	static boolean simulate = Boolean.parseBoolean( ConfigManager4Nepenthes.getSmirfMap().get("SIMULATE"));

	static String utcAddPrefix = ConfigManager4Nepenthes.getSmirfMap().get("NEPENTHES_UTC_PREFIX");
	static String end= ConfigManager4Nepenthes.getSmirfMap().get("NEPENTHES_END"); 
	static String rsync= ConfigManager4Nepenthes.getSmirfMap().get("NEPENTHES_RSYNC_PREFIX");
	static String srcName = ConfigManager4Nepenthes.getSmirfMap().get("NEPENTHES_SRCNAME_PREFIX");
	static String removeUTCPrefix = ConfigManager4Nepenthes.getSmirfMap().get("NEPENTHES_REMOVE_UTC_PREFIX");
	static String restartUTCPrefix = ConfigManager4Nepenthes.getSmirfMap().get("NEPENTHES_RESTART_UTC_PREFIX");


	static Integer basePort = Integer.parseInt(ConfigManager4Nepenthes.getMopsrMap().get("SMIRF_NEPENTHES_SERVER"));
	static Integer interNepenthesBasePort = Integer.parseInt(ConfigManager4Nepenthes.getMopsrMap().get("SMIRF_INTER_NEPENTHES_SERVER"));



	static String pointsFileNameSuffix = ConfigManager4Nepenthes.getSmirfMap().get("POINTS_FILE_EXT");
	static String rsyncFileNameSuffix = ConfigManager4Nepenthes.getSmirfMap().get("RSYNC_FILE_EXT");
	static String srcNameFileNameSuffix = ConfigManager4Nepenthes.getSmirfMap().get("SRCNAME_FILE_EXT");

	static Integer server;



	static final LinkedList<String> utcsToSearch = new LinkedList<>(); 
	
	static Pair<String, MyExecuteResultHandler> runningSMIRFSoup = null;
	static Pair<String, ExecuteWatchdog> runningWatchDog = null;
	
	static final Object runningSMIRFsoupLock = new Object();
	
	
	static ProcessDestroyer processDestroyer = new ShutdownHookProcessDestroyer();

	static ServerSocket listener = null;
	static String 	thisHost = "";
	static Integer	thisPort = -1;
	static Integer bsID = -1;

	static private boolean shutdown = false;


	public static void main(String[] args) throws UnknownHostException, ParseException {

		org.apache.commons.cli.CommandLine line;
		CommandLineParser parser = new DefaultParser();

		Options options = new Options();
		Option hostOption = new Option("H", "host", true, " pretend as host");
		Option bsIDOption = new Option("b", "bs_id", true, " Beam stitcher id");

		options.addOption(hostOption);
		options.addOption(bsIDOption);
		line = parser.parse(options, args);

		if(!hasOption(line, bsIDOption)){
			log("Required option: -b <bs_id>. Aborting now");
			System.exit(-1);
		}

		bsID = Integer.parseInt(getValue(line, bsIDOption));

		/***
		 *  get host name and get the server number. Assumes the only numbers in the host name is the server number. 
		 *  eg: mpsr-bf08.obs.molonglo.local returns 08 as the server name.
		 */

		String hostname = Inet4Address.getLocalHost().getHostName();

		if(simulate  && hasOption(line, hostOption)) hostname = getValue(line, hostOption);

		if(hostname.contains(".")) hostname = hostname.substring(0,hostname.indexOf("."));

		boolean found = false;


		for(Entry<String, List<Integer>> entry : ConfigManager4Nepenthes.getActiveBSForNodes().entrySet()){

			String iHost = entry.getKey();

			if(entry.getValue().contains(bsID)){

				if(! iHost.equals(hostname)){
					log("host:" , hostname , "tried to find:",bsID , "but was found in host:", iHost,". Likely problem with bs_config file. Aborting.");
					System.exit(-1);
				} else{
					found = true;
				}

			}
		}

		if(!found)	{
			log("This BS:", bsID," is not active in the config file on host:", hostname ,". Aborting.");
			System.exit(0);
		}

		Integer port = basePort + bsID;

		pointsFileNameSuffix = ".BS" + String.format("%02d", bsID)  + pointsFileNameSuffix;
		rsyncFileNameSuffix = ".BS" + String.format("%02d", bsID) + rsyncFileNameSuffix;
		srcNameFileNameSuffix = ".BS" + String.format("%02d", bsID) + srcNameFileNameSuffix;
		thisHost = hostname;
		thisPort = port;

		log("Started"); 


		/***
		 * start the SMIRFsouping thread.
		 * Also add the shutdown hook to gracefully end processes.
		 */
		SMIRFSoupManager.start();
		log("Started SMIRFsoup thread");
		
		interNepenthesServer.start();
		log("Started inter nepenthes server");

		addShutDownHook();
		log("Added Shutdown hook");



		try {

			listener = new ServerSocket(port);

			log("Nepenthes listening on port" ,port);

			while (!shutdown) {

				Socket socket = listener.accept();

				log("Accepted new connection from " + socket.getInetAddress().getHostName());

				try {

					PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
					BufferedReader in = new BufferedReader( new InputStreamReader(socket.getInputStream()));
					String input = in.readLine();

					if(input.contains(ConfigManager4Nepenthes.getSmirfMap().get("NEPENTHES_STATUS_PREFIX"))){
						synchronized (utcsToSearch) {
							out.println(utcsToSearch.size());
						}
						out.close();
						in.close();

					}

					else if(input.contains(removeUTCPrefix)){

						String utc = input.replaceAll(removeUTCPrefix, "").trim();

						String runningUTC = null;

						synchronized (runningSMIRFsoupLock) {
							if(runningSMIRFSoup!=null)
							runningUTC =runningSMIRFSoup.getValue0();
						}

						synchronized (utcsToSearch) {
							if(utcsToSearch.contains(utc)){

								if(runningUTC!=null && runningUTC.equals(utc)){
									out.println("Cannot remove running UTC. Please restart backend");
								}
								else{
									if(utcsToSearch.remove(utc)){
										out.println("removed UTC = " + utc);
										System.err.println("removed UTC = " + utc);
									}

									else{

										out.println("No UTC found on queue UTC = " + utc);
										System.err.println("No UTC found on queue UTC = " + utc);
									}
								}

							}
						}

						out.close();
						in.close();

					}
					
					else if(input.contains(restartUTCPrefix)){
						
						String utc = input.replaceAll(restartUTCPrefix, "").trim();
						
						synchronized (utcsToSearch) {
							utcsToSearch.add(utc);
							log("Readding UTC=",utc,"to queue");
						}
						out.close();
						in.close();

					}

					else if(input.contains(utcAddPrefix)){

						String utc = input.replaceAll(utcAddPrefix, "").trim();

						File smirfUtcDir = new File(Utilities.createDirectoryStructure(smirfBase,ConfigManager4Nepenthes.getSmirfMap().get("BS_DIR_PREFIX")+String.format("%02d", bsID),utc));
						if(!smirfUtcDir.exists()) smirfUtcDir.mkdirs();


						String pointsFile = utc+pointsFileNameSuffix;

						String rsyncFile = utc+rsyncFileNameSuffix;

						String srcNameFile = utc+srcNameFileNameSuffix;

						log("writing points file: " + pointsFile + " for utc: " + utc + " at: " + smirfUtcDir); 

						BufferedWriter bw = new BufferedWriter(new FileWriter(new File(smirfUtcDir, pointsFile)));


						while((input = in.readLine()) != null && !input.contains(rsync)){
							bw.write(input);
							bw.newLine();
							bw.flush();
						}

						bw.close();

						bw = new BufferedWriter(new FileWriter(new File(smirfUtcDir, rsyncFile)));
						while((input = in.readLine()) != null && !input.contains(srcName)){
							bw.write(input); 
							bw.newLine();
							bw.flush();

						}
						bw.close();

						bw = new  BufferedWriter(new FileWriter(new File(smirfUtcDir, srcNameFile)));
						while((input = in.readLine()) != null && !input.contains(end)){
							bw.write(input);
							bw.newLine();
							bw.flush();
						}
						bw.close();


						log("Wrote points, rsync and srcname files for: " + utc);

						synchronized (utcsToSearch) {
							utcsToSearch.add(utc);
							log("Added UTC=",utc,"to queue");
						}

						out.close();
						in.close();
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

				/**
				 * If not the first run or the present run is not complete, do not do anything.
				 */
				boolean flag = false;
				
				synchronized (runningSMIRFsoupLock) {
					flag = runningSMIRFSoup == null || runningSMIRFSoup.getValue1().isComplete();
				}
				/**
				 * SMIRF is either null or complete when flag = true;
				 */
				if(flag) {

					/**
					 * If run is complete, remove it and add the next UTC. If null, just add the next UTC.
					 */
					
					String removeUTC = null;
					synchronized (runningSMIRFsoupLock) {

						if(runningSMIRFSoup != null && runningSMIRFSoup.getValue1().isComplete()) {
							removeUTC = runningSMIRFSoup.getValue0();
							System.err.println(runningSMIRFSoup.getValue0() + " " + runningSMIRFSoup.getValue1().status );
							runningSMIRFSoup = null;	
						}

					}
					if(removeUTC !=null) {
						synchronized (utcsToSearch) {
							utcsToSearch.remove(removeUTC);
						} 
					}

					String nextUTC = null;
					synchronized (utcsToSearch) {

						if( utcsToSearch.size() > 0 ) nextUTC = utcsToSearch.getFirst();

					}

					if( nextUTC!=null) {

						log("Considering",nextUTC,"for SMIRFsouping");

						log("Asking other nodes for their status");

						for(Entry<String, Map<Integer, Integer>> nodeBSEntry : ConfigManager4Nepenthes.getInterNepenthesServers().entrySet()){

							String node = nodeBSEntry.getKey();

							for(Entry<Integer, Integer> bsPortEntry : nodeBSEntry.getValue().entrySet()){

								Integer port = bsPortEntry.getValue();
								
								if(bsPortEntry.getKey().equals(bsID)){
									log("Skipping checking myself - ",bsID,":",port);
									continue;
								}
								 
								log("Checking BS",bsPortEntry.getKey(),"on port", port);

								while(true) { 

									Socket socket = null;

									try {

										socket = new Socket();
										socket.connect(new InetSocketAddress(node, port),10000); 

										BufferedReader in = new BufferedReader( new InputStreamReader(socket.getInputStream()));	

										String utc = in.readLine();

										/**
										 * if utc is none, there is nothing running on the node, so can check the next node. Else try after 1 second.
										 * if the utc is non-none, wait until it is none or the next utc under consideration 
										 */

										if( utc != null &&  ( utc.equals("none") || utc.equals(nextUTC) ) ) {
											log("BS",bsPortEntry.getKey(),"had finished running previous UTC");
											break;
										}

										else{

											log("Waiting for UTC=",utc ," to end on BS", bsPortEntry.getKey());
											Thread.sleep(1000);
										}
										in.close();
										socket.close();
									} catch (IOException | InterruptedException e) {
										
										e.printStackTrace();
										
									} finally{
										
										if(socket!=null)
											try {
												socket.close();
											} catch (IOException e) {
												e.printStackTrace();
											}
									} // try catch finally
									
								} // while(true)


							} // bs-port map


						} // node - bs - port map 

						MyExecuteResultHandler resultHandler = null;

						try {
							resultHandler = startSMIRFSoupForUTC(nextUTC); 
						} catch (IOException e) { 
							e.printStackTrace();
						}

						synchronized (runningSMIRFsoupLock) {
							runningSMIRFSoup = new Pair<String, MyExecuteResultHandler>(nextUTC, resultHandler);

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




	static Thread interNepenthesServer = new Thread(new Runnable() {

		@Override
		public void run() {

			ServerSocket interNepenthesListener = null;
			Integer port = interNepenthesBasePort + bsID;

			try{

				interNepenthesListener = new ServerSocket(port);

				log("Internepenthes server for BS",bsID,"listening on port",port);

				while(!shutdown){

					Socket socket = null;

					try{
						
						socket =interNepenthesListener.accept();
						
						log("Accepted new connection from " + socket.getInetAddress().getHostName());

						PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
						synchronized (runningSMIRFsoupLock) {

							if(runningSMIRFSoup != null) { 
								out.println(runningSMIRFSoup.getValue0());
							}
							else {
								out.println("none");
							}
							
							log("sending ", runningSMIRFSoup != null ? runningSMIRFSoup.getValue0() : "none", "to",socket.getInetAddress().getHostName()  );
						}

						out.flush();
						out.close();


					}catch (Exception e) {
						e.printStackTrace();
					}finally {
				
						if(socket !=null) socket.close();

					}


				}


			}catch (Exception e) {
				e.printStackTrace();
			}finally {
				try {
					listener.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		}
	});



	public static void addShutDownHook(){
		Runtime.getRuntime().addShutdownHook(new Thread( new Runnable() {

			@Override
			public void run() {
				log("Preparing shutdown...");
				shutdown = true;
				if(runningWatchDog !=null && runningWatchDog.getValue1()!=null) runningWatchDog.getValue1().destroyProcess();
				while(SMIRFSoupManager.isAlive());
			}
		}));
	}


	public static MyExecuteResultHandler startSMIRFSoupForUTC(String utc) throws IOException{



		CommandLine commandLine = new CommandLine(ConfigManager4Nepenthes.getSmirfMap().get("SMIRF_BIN_DIR") + "/SMIRFsoup");
		commandLine.addArgument("-i");
		commandLine.addArgument(utc);
		commandLine.addArgument("-Z");
		commandLine.addArgument("-A");
		commandLine.addArgument(archivesBase); 
		commandLine.addArgument("-b");
		commandLine.addArgument(bsID+"");


		log("Waiting for obs.completed on all BP dirs");

		for(String bp: ConfigManager4Nepenthes.getThisBeamProcessorDirs()){
			String bpDirectoryName = Utilities.createDirectoryStructure(archivesBase, bp, utc, ConfigManager4Nepenthes.getSmirfMap().get("FB_DIR"));
			File bpDir = new File(bpDirectoryName);
			int count = 0;
			if(!bpDir.exists()) bpDir.mkdirs();

			while(true) {

				try {
					File[] files = bpDir.listFiles(new FilenameFilter() {

						@Override
						public boolean accept(File dir, String name) {
							return name.endsWith(ConfigManager4Nepenthes.getSmirfMap().get("SMIRFSOUP_TRIGGER"));
						}
					});
					if(files.length > 0 ) break;
					if(shutdown) break;

					if(count++ % 10 == 0) log(bpDirectoryName + " not completed yet...");

					Thread.sleep(2000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

			}
			if(shutdown) break;

		}
		if(shutdown) return null;

		String cmd = commandLine.getExecutable() + " ";
		for(String s: Arrays.asList(commandLine.getArguments())) cmd += s + " ";

		log("Running ",cmd);

		ExecuteWatchdog watchDog = new ExecuteWatchdog(Integer.parseInt(ConfigManager4Nepenthes.getSmirfMap().get("NEPENTHES_SMIRFSOUP_WATCHDOG")));

		Map<String, String> env = EnvironmentUtils.getProcEnvironment();

		env.put("LD_LIBRARY_PATH",ConfigManager4Nepenthes.getSmirfMap().get("GCC_ROOT")+"/lib64:"
				+ ConfigManager4Nepenthes.getSmirfMap().get("DEDISP_MULTI_ROOT") + "/lib:"
				+ ConfigManager4Nepenthes.getSmirfMap().get("CUDA_ROOT") + "/lib64:"
				+ env.getOrDefault("LD_LIBRARY_PATH", "."));

		env.put("CUDA_VISIBLE_DEVICES", ConfigManager4Nepenthes.getMopsrBsMap().get("BS_GPU_ID_"+ bsID));


		DefaultExecutor executor = new DefaultExecutor();
		executor.setWatchdog(watchDog);

		executor.setProcessDestroyer(processDestroyer);

		MyExecuteResultHandler resultHandler =  new MyExecuteResultHandler();

		executor.execute(commandLine, env, resultHandler);
		
		runningWatchDog = new Pair<String, ExecuteWatchdog>(utc, watchDog);

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



	public static synchronized void log(Object...any){
		String s = "Nepenthes on (" + thisHost + ") : ";

		for(Object o : any) 	s += o.toString() + " ";

		System.err.println(s);
	}
}
