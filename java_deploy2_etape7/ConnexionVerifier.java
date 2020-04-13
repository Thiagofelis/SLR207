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
  public ConnexionVerifier(String tm, FileWriter out, AtomicInteger validMachines)
  {
    targetMachine = tm;
    fw = out;
    numValidMachines = validMachines;
  }
  public void run()
  {
    try {
      ProcessBuilder pb = new ProcessBuilder("ssh", targetMachine, "mkdir", "-p", "/tmp/tcesar", "&&", "scp",
                        "/cal/homes/tcesar/MesDocuments/SLR205/java_slave_etape4/Slave.jar", "/tmp/tcesar/Slave.jar",
                        "&&", "echo", "ok");
      pb.redirectErrorStream(true);
      Process p = pb.start();
      p.waitFor(25, TimeUnit.SECONDS);
      try {Thread.sleep(4000);} catch (Exception e) {}
      BufferedReader reader = new BufferedReader (new InputStreamReader (p.getInputStream()));
      if (reader.ready() == false)
      {
        return;
      }
      String outs = reader.readLine();
      if (outs.equals("ok"))
      {
        numValidMachines.incrementAndGet();
        synchronized(fw)
        {
          fw.write(targetMachine + '\n');
        }

      }
    } catch (Exception e) {}
  }
}
