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
import org.apache.commons.exec.environment.EnvironmentUtils;
import org.javatuples.Pair;

public class Nepenthes{

	static String archivesBase = ConfigManager4Nepenthes.getSmirfMap().get("ARCHIVES_BASE");
	static String smirfBase = ConfigManager4Nepenthes.getSmirfMap().get("SMIRF_BASE");

	static boolean simulate = Boolean.parseBoolean( ConfigManager4Nepenthes.getSmirfMap().get("SIMULATE"));

	static String utcPrefix = ConfigManager4Nepenthes.getSmirfMap().get("NEPENTHES_UTC_PREFIX");
	static String end= ConfigManager4Nepenthes.getSmirfMap().get("NEPENTHES_END");

	static Integer basePort = Integer.parseInt(ConfigManager4Nepenthes.getMopsrMap().get("SMIRF_NEPENTHES_SERVER"));


	static String fileNameSuffix = ConfigManager4Nepenthes.getSmirfMap().get("POINTS_FILE_EXT");

	static Integer server;
	static Integer MAX_SMIRFSOUP_JOBS = 3;

	static LinkedList<String> utcsToSearch = new LinkedList<>(); 
	static Pair<String, MyExecuteResultHandler> runningSMIRFSoup = null;
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

		fileNameSuffix = "." + hostname + fileNameSuffix;
		
		thisHost = hostname;
		thisPort = port;
		
		log("Started"); 


		/***
		 * start the SMIRFsouping thread.
		 * Also add the shutdown hook to gracefully end processes.
		 */
		SMIRFSoupManager.start();
		log("Started SMIRFsoup thread");

		addShutDownHook();
		log("Added Shutdown hook");

		try {

			listener = new ServerSocket(port);

			log("listening on port");
			
			while (!shutdown) {

				Socket socket = listener.accept();
				
				log("Accepted new connection from " + socket.getInetAddress().getHostName());
				
				try {

					PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
					BufferedReader in = new BufferedReader( new InputStreamReader(socket.getInputStream()));
					String input = in.readLine();

					if(input.contains(utcPrefix)){

						String utc = input.replaceAll(utcPrefix, "").trim();

						File smirfUtcDir = new File(createDirectoryStructure(smirfBase,ConfigManager4Nepenthes.getSmirfMap().get("BS_DIR_PREFIX")+String.format("%02d", bsID),utc));
						if(!smirfUtcDir.exists()) smirfUtcDir.mkdirs();


						String pointsFile = utc+fileNameSuffix;
						
						log("writing points file: " + pointsFile + " for utc: " + utc + " at: " + smirfUtcDir);

						BufferedWriter bw = new BufferedWriter(new FileWriter(new File(smirfUtcDir, pointsFile)));


						while((input = in.readLine()) != null && !input.contains(end)){
							bw.write(input);
							bw.newLine();
							bw.flush();
						}

						if(! socket.isClosed())  out.println("received");
						out.close();
						in.close();
						bw.close();
						
						log("Wrote points file for: " + utc);
						
						synchronized (utcsToSearch) {
							utcsToSearch.add(utc);
							log("Added UTC=",utc,"to queue");
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

							String utc = utcsToSearch.getFirst();
							
							log("Considering",utc,"for SMIRFsouping");
							
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
				log("Preparing shutdown...");
				shutdown = true;
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

		log("Running ",commandLine.getExecutable(),"with args:" + Arrays.asList(commandLine.getArguments()));

		log("Waiting for obs.completed on all BP dirs");

		for(String bp: ConfigManager4Nepenthes.getThisBeamProcessorDirs()){
			String bpDirectoryName = createDirectoryStructure(archivesBase, bp, utc, ConfigManager4Nepenthes.getSmirfMap().get("FB_DIR"));
			File bpDir = new File(bpDirectoryName);
			int count = 0;
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
		}

		ExecuteWatchdog watchDog = new ExecuteWatchdog(4 * 15 * 60 * 1000);

		Map<String, String> env = EnvironmentUtils.getProcEnvironment();
		
		env.put("LD_LIBRARY_PATH",ConfigManager4Nepenthes.getSmirfMap().get("GCC_ROOT")+"/lib64:"
								+ ConfigManager4Nepenthes.getSmirfMap().get("DEDISP_MULTI_ROOT") + "/lib:"
								+ ConfigManager4Nepenthes.getSmirfMap().get("CUDA_ROOT") + "/lib64:"
								+ env.getOrDefault("LD_LIBRARY_PATH", "."));
		
		DefaultExecutor executor = new DefaultExecutor();
		executor.setWatchdog(watchDog);
	
		executor.setProcessDestroyer(processDestroyer);

		MyExecuteResultHandler resultHandler =  new MyExecuteResultHandler();

		executor.execute(commandLine, env, resultHandler);

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
	
	public static String createDirectoryStructure(String...names){
		String result = "";
		for( int i=0; i< names.length; i++){
			result += names[i];
			if(i != names.length -1 ) result += "/";
		}
		return result;
	}
	
	public static synchronized void log(Object...any){
		String s = "Nepenthes on (" + thisHost + ":" + thisPort + ") : ";
		
		for(Object o : any) 	s += o.toString() + " ";
		
		System.err.println(s);
	}
}
