import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.*;

public class Master
{
	public static void main(String[] args) throws Exception
	{
		String command = "cd /tmp/tcesar ; java -jar Slave.jar" + " " + args[0] + " " + args[1];
		// ^takes as parameters the time that will take the slave to do the calculations
		//  and an integer that signalizes that an error will be produced
		ProcessBuilder pb = new ProcessBuilder("bash", "-c", command);
		pb.redirectErrorStream(true);
		Process p = pb.start();

		boolean b = p.waitFor(3, TimeUnit.SECONDS);
		if (!b)
		{
			System.out.println("timeout");
			return;
		}

		BufferedReader reader = new BufferedReader (new InputStreamReader (p.getInputStream()));

		String line;
    while ((line = reader.readLine()) != null)
		{
        System.out.println(line);
    }
		
	}
}
