//package DeployPack;
import java.io.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.ArrayList;

public class ConnexionVerifier extends Thread
{
	int timeOut = 60;
  FileWriter fw;
  String targetMachine;
  AtomicInteger numValidMachines;
	boolean followingMode;
  public ConnexionVerifier(String tm, FileWriter out, AtomicInteger validMachines, boolean _followingMode)
  {
		followingMode = _followingMode;
    targetMachine = tm;
    fw = out;
    numValidMachines = validMachines;
  }
  public void run()
  { // we sync with a shared variable when printing in stdout to prevent the outputs from different threads from getting mixed
    try {
      String[] command = {"ssh", targetMachine, "hostname"};

      ProcessBuilder pb = new ProcessBuilder(command);
      pb.redirectErrorStream(true);
      long startTime = System.nanoTime();
      Process p = pb.start();
      
      p.waitFor(timeOut, TimeUnit.SECONDS);
      long endTime = System.nanoTime();
      BufferedReader reader = new BufferedReader (new InputStreamReader (p.getInputStream()));
      if (reader.ready() == false)
      {
				if (followingMode)
					synchronized (numValidMachines)
					{
						System.out.println(targetMachine + " didnt answer after " + ((endTime - startTime) / 1000000) + " ms");
					}
        return;
      }
      String outs = reader.readLine();
      if (outs.equals(targetMachine))
      {
        numValidMachines.incrementAndGet();
				fw.write(targetMachine + '\n');				

				if (followingMode)
					synchronized(fw)
					{
						System.out.println(targetMachine + " validated after " + ((endTime - startTime) / 1000000) + " ms");
					}
      }
      else
				if (followingMode)
					synchronized (numValidMachines)
					{
						System.out.println(targetMachine + " returned the following error after " + ((endTime - startTime) / 1000000) + " ms:");
						System.out.println(outs);
						while( (outs = reader.readLine()) != null) System.out.println(outs);
					}
      
    } catch (Exception e) { e.printStackTrace(); }
  }
}
