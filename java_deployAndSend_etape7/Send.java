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
  public ConnexionVerifier(String tm, FileWriter out)
  {
    targetMachine = tm;
    fw = out;
  }
  public void run()
  {
    try {
      String[] command = new String[5];
  //    String[] command = ["ssh", targetMachine, "hostname"];
      command[0] = "ssh"; command[1] = targetMachine; command[2] = "mkdir"; command[3] = "-p";
      command[4] = "/tmp/tcesar";
//System.out.println(command);
      ProcessBuilder pb = new ProcessBuilder(command);
    //  pb.redirectErrorStream(true);

      Process p = pb.start();
      p.waitFor(20, TimeUnit.SECONDS);

      BufferedReader reader = new BufferedReader (new InputStreamReader (p.getInputStream()));
      if (reader.ready() == false)
      {
        return;
      }
      String outs = reader.readLine();
      if (outs.equals("")) //success
      {
        String[] command2 = new String[3];
        command2[0] = "scp"; command2[1] = "../java_slave_etape5/Slave.jar";
        command2[2] = targetMachine + ":/tmp/tcesar/Slave.java";
        ProcessBuilder pb2 = new ProcessBuilder(command2);
        Process p2 = pb2.start();
        p2.waitFor(20, TimeUnit.SECONDS);
            
      }
      else
      {
        return;
      }
    } catch (Exception e) {}
  }
}
