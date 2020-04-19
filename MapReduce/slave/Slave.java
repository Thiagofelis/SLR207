import java.io.*;
import java.util.*;
import java.util.concurrent.*;

class Slave
{
	static int timeOut = 300;
  public static void main(String[] args)
  {
    if (args[0].compareTo("0") == 0)
    {
      map("test.txt", "test.txt");
    }
    else if (args[0].compareTo("1") == 0)
    {
      String[] machines = getMachinesArray();
      prepareShuffle("test.txt", machines);
      zip();
      sendZips(machines);
    }
    else if (args[0].compareTo("2") == 0)
    {
			unzip();
      reduce();
      sendReducesToMaster();
    }
  }


	static void sendReducesToMaster()
  {
		try{
			ProcessBuilder pb = new ProcessBuilder("bash", "/tmp/tcesar/send_reduces.sh");
			pb.redirectErrorStream(true);
			Process p = pb.start();
			p.waitFor(timeOut, TimeUnit.SECONDS);
  	  BufferedReader reader = new BufferedReader(new InputStreamReader (p.getInputStream() ));
			if (reader.ready() == false)
			{
	      System.out.println ("error sending reduce : buffer not ready");
	      return;
			}
			String outs = reader.readLine();
			if (!outs.equals("ok"))
			{
				System.out.println("error sending reduce : output below");
				System.out.println(outs);
				while (reader.ready() && (outs = reader.readLine()) != null)
					System.out.println(outs);
			}
		} catch (Exception e) {e.printStackTrace(); }
  }

  static String[] getMachinesArray()
  {
    List<String> list = null;
    try{
      File file = new File("/tmp/tcesar/machines.txt");
      BufferedReader br = new BufferedReader(new FileReader(file));

      String str;
      list = new ArrayList<String>();

      while ((str = br.readLine()) != null)
      {
        list.add(str);
      }
      br.close();
    } catch(Exception e) {e.printStackTrace(); }
    return list.toArray(new String[0]);
  }

  static void sendFile(String file, String dest)
  { // sends a shuffle to another slave
    try {
			while (true){
				String[] command = {"bash", "-c", "scp /tmp/tcesar/shuffles/" + file + " " + dest + ":/tmp/tcesar/shufflesreceived/" + file + " ; echo ok"};
				ProcessBuilder pb = new ProcessBuilder(command);
				pb.redirectErrorStream(true);
				Process p = pb.start();
				p.waitFor(timeOut, TimeUnit.SECONDS);

				BufferedReader reader = new BufferedReader(new InputStreamReader (p.getInputStream() ));
				if (reader.ready() == false)
				{
					System.out.println ("error sending file to " + dest + " : out buffer not ready");
					return;
				}
				String outs = reader.readLine();
				// if the command doesn't return a 'ok', the error that almost always happens is due to the fact that some slaves get saturated with ssh requests. we repeat the request until it gets accepted
				if(outs.equals("ok"))
					return;
			}

    } catch (Exception e) {e.printStackTrace(); }
  }

  static int hashFunction(String s)
  { // fixes a design error of the abs function : in a particular situation it can return a negative number
    return (s.hashCode() == Integer.MIN_VALUE) ? 0 : Math.abs(s.hashCode());
  }

	static void zip()
	{
		try{
	    ProcessBuilder pb = new ProcessBuilder("bash", "/tmp/tcesar/shuffles/zip.sh", java.net.InetAddress.getLocalHost().getHostName());
	    
	    pb.redirectErrorStream(true);
	    Process p = pb.start();
	    p.waitFor(timeOut, TimeUnit.SECONDS);
	    BufferedReader reader = new BufferedReader (new InputStreamReader (p.getInputStream() ));
	    if (reader.ready() == false)
	    {
				System.out.println("error during zipping - buffer not ready");
	    }
	    String outs = reader.readLine();
	    if (!outs.equals("ok"))
	    {
				System.out.println("error during zipping - output below: ");
				System.out.println(outs);
				while ((outs = reader.readLine()) != null)
					System.out.println(outs);
	    }
		}catch (Exception e) { e.printStackTrace();}
	}
	static void unzip()
	{
		try{
	    ProcessBuilder pb = new ProcessBuilder("bash", "/tmp/tcesar/shufflesreceived/unzip.sh");
	    
	    pb.redirectErrorStream(true);
	    Process p = pb.start();
	    p.waitFor(20, TimeUnit.SECONDS);
	    BufferedReader reader = new BufferedReader (new InputStreamReader (p.getInputStream() ));
	    if (reader.ready() == false)
	    {
				System.out.println("error during unzipping - 1");
	    }
	    String outs = reader.readLine();
	    if (!outs.equals("ok"))
	    {
				System.out.println("error during unzipping - 2:");
				System.out.println(outs);
				while ((outs = reader.readLine()) != null)
					System.out.println(outs);
	    }
		}catch (Exception e) { e.printStackTrace();}
	}

	static void sendZips(String[] machines)
	{
		try {
	    File dir = new File("/tmp/tcesar/shuffles");
	    File[] files = dir.listFiles();
	    String machineID;
	    for (File child : files)
	    { // the files on the dir shuffles are all .zip - with the exception of zip.sh, which we skip. we send them to their respective targets
				if (child.getName().equals("zip.sh"))
					continue;
				if (child.isFile())
		    {
					machineID = child.getName().split("@")[0]; // on the zip name, we have the target machine id before the @
					sendFile(child.getName(), machines[Integer.parseInt(machineID)]);
		    }
	    }
		} catch (Exception e) {e.printStackTrace();}
	}
    
	static void prepareShuffle(String maps, String[] machines)
  {
    try{
      File file = new File("/tmp/tcesar/maps/" + maps);
      BufferedReader br = new BufferedReader(new FileReader(file));

      String str;
      String[] splited = null;
      FileWriter fw = null;
      int hash;
			// we iterate the file produced during the map phase. for each word occurence, we calculate its hash and divide by the number of machines. the result gives us the machine to which the word will be sent. for each machine, there's a folder on the dir shuffles that was created during the deploy phase, in which the file will be stored
      while ((str = br.readLine()) != null)
      {
        splited = str.split(" ");
        hash = hashFunction(splited[0]);
        fw = new FileWriter("/tmp/tcesar/shuffles/" + Integer.toString(hash % machines.length) + "/" + Integer.toString(hash) + "-" + java.net.InetAddress.getLocalHost().getHostName() + ".txt", true);
        fw.write(splited[0] + " 1\n");
        fw.close();
			}
      br.close();
    } catch(Exception e) {e.printStackTrace(); }
  }
  static void map(String in, String out)
  {
    try {
      File file = new File("/tmp/tcesar/splits/" + in);
      BufferedReader br = new BufferedReader(new FileReader(file));

      FileWriter fw = new FileWriter("/tmp/tcesar/maps/" + out);

      int i;
      String str;
      String[] splited = null;

      while ((str = br.readLine()) != null)
      {
        splited = str.split(" ");

        // is the line blank?
        if (splited.length == 0)
          continue;
        if (splited[0].compareTo("") == 0)
          continue;

        for (i = 0; i < splited.length; i++)
        {
          fw.write(splited[i] + " 1\n");
        }
      }
      fw.close();
      br.close();
    } catch (Exception e) {e.printStackTrace(); }
  }
  static void reduce()
  {
    try {
      File dir = new File("/tmp/tcesar/shufflesreceived/files");
      File[] directoryListing = dir.listFiles();

      Map<String, Integer> map = new HashMap<String, Integer>();

      BufferedReader br = null;

      String str;
      String[] splited = null;

      if (directoryListing != null)
      {
        for (File child : directoryListing)
        {
          br = new BufferedReader(new FileReader(child));
          while ((str = br.readLine()) != null)
          {
            splited = str.split(" ");
            // is the line blank?
            if (splited.length == 0)
              continue;
            if (splited[0].compareTo("") == 0)
              continue;

            if (map.get(splited[0]) == null)
            {
              map.put(splited[0], 1);
            }
            else
            {
              map.put(splited[0], map.get(splited[0]) + 1);
            }
          }
          br.close();
        }
      }

      FileWriter fw = null;

      for (Map.Entry<String, Integer> entry : map.entrySet())
      {
        fw = new FileWriter("/tmp/tcesar/reduces/" + Integer.toString(hashFunction(entry.getKey())) + ".txt");
        fw.write(entry.getKey() + " " + Integer.toString(entry.getValue()) + "\n");
        fw.close();
      }

    } catch (Exception e) {e.printStackTrace(); }
  }
}
