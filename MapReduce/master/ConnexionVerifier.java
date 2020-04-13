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
      ProcessBuilder pb = new ProcessBuilder("ssh", targetMachine, "hostname");
      pb.redirectErrorStream(true);
      Process p = pb.start();
      p.waitFor(5, TimeUnit.SECONDS);

      BufferedReader reader = new BufferedReader (new InputStreamReader (p.getInputStream()));
      if (reader.ready() == false)
      {
        return;
      }
      String outs = reader.readLine();
      if (outs.equals(targetMachine))
      {

        numValidMachines.incrementAndGet();
        synchronized(fw)
        {
          fw.write(targetMachine + '\n');
        }

      }
    } catch (Exception e) { e.printStackTrace(); }
  }
}
