//package DeployPack;
import java.io.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.ArrayList;

public class ConnexionVerifier extends Thread
{
  FileWriter fw;
  String targetMachine;
  AtomicInteger numValidMachines;
  public ConnexionVerifier(String tm, FileWriter out, AtomicInteger validMachines)
  {
    targetMachine = tm;
    fw = out;
    numValidMachines = validMachines;
  }
  public void run()
  {
    try {

      String[] command = {"ssh", targetMachine, "hostname"};
      ProcessBuilder pb = new ProcessBuilder(command);
      pb.redirectErrorStream(true);
      long startTime = System.nanoTime();
      Process p = pb.start();
      
      p.waitFor(60, TimeUnit.SECONDS);
      long endTime = System.nanoTime();
      BufferedReader reader = new BufferedReader (new InputStreamReader (p.getInputStream()));
      if (reader.ready() == false)
      {
	  synchronized (numValidMachines) {System.out.println(targetMachine + " didnt answer after " + ((endTime - startTime) / 1000000) + " ms");}
        return;
      }
      String outs = reader.readLine();
      if (outs.equals(targetMachine))
      {
        numValidMachines.incrementAndGet();
        synchronized(fw)
        {
	    System.out.println(targetMachine + " validated after " + ((endTime - startTime) / 1000000) + " ms");
          fw.write(targetMachine + '\n');
        }
	return;
      }
      synchronized (numValidMachines)
      {
	  System.out.println(targetMachine + " returned the following error after " + ((endTime - startTime) / 1000000) + " ms:");
	  System.out.println(outs);
	  while( (outs = reader.readLine()) != null) System.out.println(outs);
      }
      
    } catch (Exception e) { e.printStackTrace(); }
  }
}
