import java.io.*;
import java.util.*;
import java.util.concurrent.*;

class Slave {
  public static void main(String[] args)
  {
    if (args[0].compareTo("0") == 0)
    {
      map(args[1], args[1]);
    }
    else if (args[0].compareTo("1") == 0)
    {
      String[] machines = getMachinesArray();
      prepareShuffle(args[1], machines);
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

    /*  static void sendReducesToMaster()
  {
    try {
      File dir = new File("/tmp/tcesar/reduces");
      File[] directoryListing = dir.listFiles();

      if (directoryListing != null)
      {
        for (File child : directoryListing)
        {
	    ProcessBuilder pb = new ProcessBuilder("bash", "-c", "scp /tmp/tcesar/reduces/" + child.getName()+" /cal/homes/tcesar/MesDocuments/SLR205/MapReduce/master/reduces/" + child.getName() + " ; echo ok");
          Process p = pb.start();
          p.waitFor(15, TimeUnit.SECONDS);
	  BufferedReader reader = new BufferedReader(new InputStreamReader (p.getInputStream() ));
	  if (reader.ready() == false)
	  {
	      System.out.println ("error sending reduce : buffer not ready");
	      return;
	  }
	  String outs = reader.readLine();
	  if(!outs.equals("ok"))
  	  {
	      System.out.println ("error sending reduce, output buffer below");
	      System.out.println(outs);
	      while (reader.ready() && (outs = reader.readLine()) != null)
		System.out.println(outs);
	  }
        }
      }
    } catch (Exception e) {e.printStackTrace(); }
  }
    */
    static void sendReducesToMaster()
  {
      try{
	  ProcessBuilder pb = new ProcessBuilder("bash", "/tmp/tcesar/send_reduces.sh");
	  pb.redirectErrorStream(true);
	  Process p = pb.start();
          p.waitFor(90, TimeUnit.SECONDS);
  	  BufferedReader reader = new BufferedReader(new InputStreamReader (p.getInputStream() ));
	  if (reader.ready() == false)
	  {
	      System.out.println ("error sending reduce : buffer not ready");
	      return;
	  }
	  String outs = reader.readLine();
	 
    	  System.out.println(outs);
	  while (reader.ready() && (outs = reader.readLine()) != null)
		System.out.println(outs);

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
  {
    try {
	while (true){
	String[] command = {"bash", "-c", "scp /tmp/tcesar/shuffles/" + file + " " + dest + ":/tmp/tcesar/shufflesreceived/" + file + " ; echo ok"};
	//String[] command = {"scp", "/tmp/tcesar/shuffles/" + file + ".txt", dest + ":/tmp/tcesar/shufflesreceived/" + file + ".txt;", "echo", "ok"};
	ProcessBuilder pb = new ProcessBuilder(command);
	pb.redirectErrorStream(true);
	Process p = pb.start();
	p.waitFor(30, TimeUnit.SECONDS);

	BufferedReader reader = new BufferedReader(new InputStreamReader (p.getInputStream() ));
	if (reader.ready() == false)
	{
	    System.out.println ("error sending file to " + dest + " : out buffer not ready");
	    return;
	}
	String outs = reader.readLine();
	/*if(!outs.equals("ok"))
	{
	    System.out.println ("error sending file to " + dest + " : output buffer below");
	    System.out.println(outs);
	    while (reader.ready() && (outs = reader.readLine()) != null)
		System.out.println(outs);
		}*/
	if(outs.equals("ok"))
	  return;
	}

    } catch (Exception e) {e.printStackTrace(); }
  }

  static int hashFunction(String s)
  {
    return (s.hashCode() == Integer.MIN_VALUE) ? 0 : Math.abs(s.hashCode());
  }
    /*
  static void prepareShuffle(String maps)
  {
    try{

      Files.lines(Path.get("/tmp/tcesar/maps/" + maps + ".txt"))
        .parallel()
        .forEach( stream -> { try
        {
          BufferedReader br = new BufferedReader(stream);
          String str = null;
          int hash;
          String[] splited = null;
          FilrWriter fw = null;
          while ((str = br.readLine()) != null)
          {
            splited = str.split(" ");
            hash = hashFunction(splited[0]);
            fw = new FileWriter("/tmp/tcesar/shuffles/" + Integer.toString(hash) + "-" +
              java.net.InetAddress.getLocalHost().getHostName() + ".txt", true);
            fw.write(splited[0] + " 1\n");
            fw.close();

            sendFile(Integer.toString(hash) + "-" +
              java.net.InetAddress.getLocalHost().getHostName(),
              machines[hash % machines.length]);
          }
          br.close();
        } catch (Exception e) {e.printStackTrace();} });

    } catch(Exception e) {e.printStackTrace(); }
  }
*/
    static void zip()
    {
	try{
	    ProcessBuilder pb = new ProcessBuilder("bash", "/tmp/tcesar/shuffles/zip.sh", java.net.InetAddress.getLocalHost().getHostName());
	    
	    pb.redirectErrorStream(true);
	    Process p = pb.start();
	    p.waitFor(20, TimeUnit.SECONDS);
	    BufferedReader reader = new BufferedReader (new InputStreamReader (p.getInputStream() ));
	    if (reader.ready() == false)
	    {
		System.out.println("error during zipping");
	    }
	    String outs = reader.readLine();
	    if (!outs.equals("ok"))
	    {
		System.out.println("error during zipping");
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
		while ((outs = reader.readLine()) != null) System.out.println(outs);
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
	    {
		if (child.getName().equals("zip.sh"))
		    continue;
		if (child.isFile())
		    {
			machineID = child.getName().split("@")[0];
			sendFile(child.getName(), machines[Integer.parseInt(machineID)]);
		    }
	    }
	} catch (Exception e) {e.printStackTrace();}
    }
    
    static void prepareShuffle(String maps, String[] machines)
  {
    try{
      File file = new File("/tmp/tcesar/maps/" + maps + ".txt");
      BufferedReader br = new BufferedReader(new FileReader(file));

      String str;
      String[] splited = null;
      FileWriter fw = null;
      int hash;

      while ((str = br.readLine()) != null)
      {
        splited = str.split(" ");
        hash = hashFunction(splited[0]);
        fw = new FileWriter("/tmp/tcesar/shuffles/" + Integer.toString(hash % machines.length) + "/" + Integer.toString(hash) + "-" + java.net.InetAddress.getLocalHost().getHostName() + ".txt", true);
        fw.write(splited[0] + " 1\n");
        fw.close();

	//        sendFile(Integer.toString(hash) + "-" + java.net.InetAddress.getLocalHost().getHostName(), machines[hash % machines.length]);
      }
      br.close();
    } catch(Exception e) {e.printStackTrace(); }
  }
  static void map(String in, String out)
  {
    try {
      File file = new File("/tmp/tcesar/splits/" + in + ".txt");
      BufferedReader br = new BufferedReader(new FileReader(file));

      FileWriter fw = new FileWriter("/tmp/tcesar/maps/" + out + ".txt");

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
