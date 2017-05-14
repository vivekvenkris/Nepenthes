
public class Utilities {
	public static String createDirectoryStructure(String...names){
		String result = "";
		for( int i=0; i< names.length; i++){
			result += names[i];
			if(i != names.length -1 ) result += "/";
		}
		return result;
	}
}
