

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.javatuples.Pair;



public class ConfigManager4Nepenthes {  

	static String smirfConfig = "/home/vivek/SMIRF/config/smirf.cfg";

	private static  Map<String, Map<Integer, Integer>>  nepenthesServers;
	private static Map<String, String> smirfMap;
	private static Map<String, String> mopsrMap;
	private static Map<String, String> mopsrBpMap;
	private static Map<String, String> mopsrBpCornerturnMap;
	private static Map<String, String> mopsrBsMap;
	// host, bp, min, max
	private static Map<String, Map<Integer, Pair<Integer, Integer > > > beamBoundariesMap = new HashMap<>();
	private static List<String> thisBeamProcessorDirs = new ArrayList<>();
	private static Map<String, List<Integer>> activeBSForNodes = new HashMap<>();

	private static String 	thisHost;	
	private static Integer numFanBeams;
	private static String  edgeNode;
	
	private static boolean loaded = false;


	static{
		try {
			thisHost = removeDomain(InetAddress.getLocalHost().getHostName());
			loadConfigs();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {
		thisHost = "mpsr-bf00"; removeDomain(InetAddress.getLocalHost().getHostName());
		loadConfigs();
	}



	static void loadConfigs() throws IOException{
		
		if(loaded) {
			System.err.println("Already loaded");
			return;
		}

		smirfMap = readConfig(smirfConfig);

		mopsrMap = readConfig( Utilities.createDirectoryStructure(smirfMap.get("CONFIG_ROOT"),smirfMap.get("MOPSR_CONFIG")));
		mopsrBpMap = readConfig(Utilities.createDirectoryStructure(smirfMap.get("CONFIG_ROOT"),smirfMap.get("MOPSR_BP_CONFIG")));
		mopsrBpCornerturnMap = readConfig(Utilities.createDirectoryStructure(smirfMap.get("CONFIG_ROOT"),smirfMap.get("MOPSR_BP_CORNERTURN_CONFIG")));
		mopsrBsMap = readConfig(Utilities.createDirectoryStructure(smirfMap.get("CONFIG_ROOT"),smirfMap.get("MOPSR_BS_CONFIG")));


		edgeNode = smirfMap.get("EDGE_NODE");

		Integer nepenthesBasePort = Integer.parseInt(mopsrMap.get("SMIRF_NEPENTHES_SERVER"));
		Integer numBS = Integer.parseInt(mopsrBsMap.get("NUM_BS"));
		

		// node, bs, port
		Map<String, Map<Integer, Integer>> nepenthesServers = new HashMap<>();

		for(int bs=0; bs< numBS; bs++){

			String nodeName = mopsrBsMap.get( String.format("BS_%d", bs) );

			if(mopsrBsMap.get(String.format("BS_STATE_%d", bs)).equals("active")){

				Map<Integer, Integer> map = nepenthesServers.getOrDefault(nodeName, new HashMap<>());
				map.put(bs,nepenthesBasePort + bs );

				nepenthesServers.put(nodeName, map);

				List<Integer> bsList = activeBSForNodes.getOrDefault(nodeName, new ArrayList<>());
				bsList.add(bs);
				activeBSForNodes.put(nodeName, bsList);
				
			}

		}

		ConfigManager4Nepenthes.nepenthesServers =  Collections.unmodifiableMap(nepenthesServers);


		numFanBeams = Integer.parseInt(mopsrBpCornerturnMap.get("NBEAM"));

		Integer numBP = Integer.parseInt(mopsrBpMap.get("NUM_BP"));

		for( int bp=0 ; bp< numBP; bp++ ) 
			if( mopsrBpMap.get("BP_" + bp).equals(thisHost) || thisHost.equals(edgeNode)) 
				thisBeamProcessorDirs.add(String.format("BP%02d", bp));

		for(Entry<String, List<Integer>> entry: activeBSForNodes.entrySet()){

			String node = entry.getKey();

			for(Integer bs: entry.getValue()){

				List<Integer> beamProcessors = new ArrayList<>();

				if(node.equals(edgeNode)) 	{
					Map<Integer, Pair<Integer, Integer>> map = beamBoundariesMap.getOrDefault(node, new HashMap<>());
					map.put(bs, new Pair<Integer, Integer>( 1 , numFanBeams));
					beamBoundariesMap.put(node, map);
					continue;
				}

				for( int bp=0 ; bp< numBP; bp++ ) if( mopsrBpMap.get("BP_" + bp).equals(node) ) beamProcessors.add(bp);
				Integer minFB = null, maxFB = null;

				for(Integer bp: beamProcessors) {

					Integer min = Integer.parseInt(mopsrBpCornerturnMap.get("BEAM_FIRST_RECV_" + bp));
					Integer max = Integer.parseInt(mopsrBpCornerturnMap.get("BEAM_LAST_RECV_" + bp));


					if(minFB == null || minFB > min)  minFB = min;
					if(maxFB == null || maxFB < max)  maxFB = max;

				}
				Map<Integer, Pair<Integer, Integer>> map = beamBoundariesMap.getOrDefault(node, new HashMap<>());
				map.put(bs, new Pair<Integer, Integer>( minFB + 1 ,  maxFB + 1));
				beamBoundariesMap.put(node, map);

			}	

		}

		loaded = true;
	}

	private static Map<String, String> readConfig(String file) throws IOException{

		Map<String, String> map = new HashMap<>();

		try {
			BufferedReader br = new BufferedReader( new FileReader( new File(file)));
			String line;
			while((line = br.readLine()) !=null){

				if(line.trim().isEmpty()) continue;

				if(line.trim().startsWith("#")) continue;

				if(line.contains("#")) line = line.substring(0, line.indexOf("#"));

				String[] chunks = line.trim().split("\\s+",2);
				map.put(chunks[0], chunks[1]);
			}
			br.close();
			return Collections.unmodifiableMap(map);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			throw e;
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		}

	}


	public static String getBeamSearcherForFB(int fb){

		for(Entry<String,Map<Integer, Pair<Integer, Integer>>> entry : beamBoundariesMap.entrySet()){

			for(Entry <Integer, Pair<Integer, Integer>> entry2: entry.getValue().entrySet()) {

				if( (fb >= entry2.getValue().getValue0()) && ( fb <= entry2.getValue().getValue1() ) ) return entry2.getKey()+"";

			}


		}
		return null;

	}

	//	public static Integer getServerNumberForFB(int fb){
	//		return  getServerNumberForServerName(getBeamSearcherForFB(fb));
	//	}

	public static  Integer getServerNumberForServerName(String name){
		return Integer.parseInt(name.replaceAll("\\D+", ""));
	}

	public static String removeDomain(String hostname){
		return (hostname.contains("."))? hostname.split("\\.")[0]: hostname; 
	}





	public static String getSmirfConfig() {
		return smirfConfig;
	}



	public static Map<String, Map<Integer, Integer>> getNepenthesServers() {
		return nepenthesServers;
	}



	public static Map<String, String> getMopsrBsMap() {
		return mopsrBsMap;
	}



	public static Map<String, List<Integer>> getActiveBSForNodes() {
		return activeBSForNodes;
	}



	public static Map<String, String> getSmirfMap() {
		return smirfMap;
	}

	public static Map<String, String> getMopsrMap() {
		return mopsrMap;
	}

	public static Map<String, String> getMopsrBpMap() {
		return mopsrBpMap;
	}

	public static Map<String, String> getMopsrBpCornerturnMap() {
		return mopsrBpCornerturnMap;
	}



	public static Integer getNumFanBeams() {
		return numFanBeams;
	}

	public static String getEdgeNode() {
		return edgeNode;
	}



	public static List<String> getThisBeamProcessorDirs() {
		return thisBeamProcessorDirs;
	}



	public static String getThisHost() {
		return thisHost;
	}



}
