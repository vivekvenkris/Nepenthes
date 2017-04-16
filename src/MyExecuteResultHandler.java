import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteResultHandler;

public class MyExecuteResultHandler implements ExecuteResultHandler {

	String status = "running";
	
	@Override
	public void onProcessFailed(ExecuteException arg0) {
		status = "fail";

	}

	@Override
	public void onProcessComplete(int arg0) {
		status = "success";
	}

	public boolean isComplete(){
		return !status.equals("running");
	}

}
