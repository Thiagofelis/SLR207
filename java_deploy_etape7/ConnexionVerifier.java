//package DeployPack;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.InputStreamReader;
import java.io.FileWriter;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.ArrayList;

import java.util.concurrent.*;
public class ConnexionVerifier extends Thread
{
  FileWriter fw;
  String targetMachine;
  AtomicInteger numValidMachines;
  ArrayList<String> workingMachines;
  public ConnexionVerifier(String tm, FileWriter out, AtomicInteger validMachines, ArrayList<String> wm)
  {
    workingMachines = wm;
    targetMachine = tm;
    fw = out;
    numValidMachines = validMachines;
  }
  public void run()
  {
    try {
      String[] command = new String[3];
  //    String[] command = ["ssh", targetMachine, "hostname"];
      command[0] = "ssh"; command[1] = targetMachine; command[2] = "hostname";
//System.out.println(command);
      ProcessBuilder pb = new ProcessBuilder(command);
    //  pb.redirectErrorStream(true);

      Process p = pb.start();
      p.waitFor(15, TimeUnit.SECONDS);

      BufferedReader reader = new BufferedReader (new InputStreamReader (p.getInputStream()));
      if (reader.ready() == false)
      {
        return;
      }
      String outs = reader.readLine();
      if (outs.equals(targetMachine))
      {
        System.out.println(targetMachine);
        numValidMachines.incrementAndGet();
        synchronized(fw)
        {
          fw.write(targetMachine + '\n');
        }
      /*  synchronized(workingMachines)
        {
          workingMachines.add(targetMachine);
        }*/
      }
    } catch (Exception e) {}
  }
}
