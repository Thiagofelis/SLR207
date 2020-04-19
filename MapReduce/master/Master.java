import java.io.*;
import java.util.concurrent.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

class Master
{
  static void waitUser(String finished, String next)
  {
    System.out.println(finished + " finished");
    System.out.println("press enter to procced to " + next);
    try {System.in.read();} catch (Exception e) {e.printStackTrace();}
  }

  public static void main(String[] args)
  { // if args[0] == 1 we are in following mode		
		if (args.length != 2)
		{
			System.out.println("please run as : java Master file (0 or 1 for following mode)");
			return;
		}
		boolean followingMode = args[1].charAt(0) == '1' ? true : false;
		
    clear();
    int numValidMachines = getValidMachines(followingMode);
    makeSplits(args[0], numValidMachines);

    if (followingMode) waitUser("split", "deploy");

    runInSlave(new String[]{"bash", "/cal/homes/tcesar/MesDocuments/SLR207/MapReduce/master/scripts/init.sh", "#", Integer.toString(numValidMachines)}, followingMode);

    if (followingMode) waitUser("deploy", "map");

    runInSlave(new String[]{"java", "-jar", "/tmp/tcesar/Slave.jar", "0"}, followingMode);

    if (followingMode) waitUser("map", "shuffle");

    runInSlave(new String[]{"java", "-jar", "/tmp/tcesar/Slave.jar", "1"}, followingMode);

    if (followingMode) waitUser("shuffle", "reduce");

    runInSlave(new String[]{"java", "-jar", "/tmp/tcesar/Slave.jar", "2"}, followingMode);

    if (followingMode) waitUser("reduce", "sort");

    sort();
  }

  static void clear()
  { // clear some directories on the master
    try {
      ProcessBuilder pb = new ProcessBuilder("bash", "scripts/clr.sh");
      Process p = pb.start();
      p.waitFor(10, TimeUnit.SECONDS);
    } catch (Exception e) {e.printStackTrace(); }
  }

  static void sort()
  {
    // defines order on the entries
    Comparator<Map.Entry<String, Integer>> comp =
			(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) ->
			{
				if (o1.getValue() > o2.getValue())
					return -1;
				if (o1.getValue() < o2.getValue())
					return 1;
				if (o1.getKey().compareTo(o2.getKey()) < 0)
					return -1;
				if (o1.getKey().compareTo(o2.getKey()) > 0)
					return 1;
				return 0;
			};

    TreeSet<Map.Entry<String, Integer>> wordSet = new TreeSet<Map.Entry<String, Integer>>(comp);

    try {
      File dir = new File("reduces");
      File[] directoryListing = dir.listFiles();
      BufferedReader br = null;

      String str;
      String[] splited = null;

			// to order the words, we read all the reduces and add each word to a treeset - which is ordered by construction. then, we just print the words in order
      if (directoryListing != null)
      {
        for (File child : directoryListing)
        {
          br = new BufferedReader(new FileReader(child));
          if ((str = br.readLine()) == null)
          {
            br.close(); continue;
          }
          splited = str.split(" ");
          wordSet.add(new AbstractMap.SimpleEntry<String, Integer> (splited[0], Integer.parseInt(splited[1])));
          br.close();
        }
      }
			
      FileWriter out = new FileWriter("output.txt");
      wordSet.forEach( (Map.Entry<String, Integer> e) ->
											 {
												 try {out.write(e.getValue() + " " + e.getKey() + "\n"); } catch (Exception ex) {ex.printStackTrace();}
											 });
      out.close();
			
    } catch (Exception e) {e.printStackTrace();}
  }

  static void makeSplits(String fileName, int numSplits)
  {
    try {
      File file = new File(fileName);
      InputStream is = new FileInputStream(file);
      FileWriter out = null;

			// the file size divided by the num of splits is a chosen as the partition size
      long avaragePartitionSize = file.length() / numSplits;
      long lastSplit = 0;
      long readerCounter = 0;

      int i;
      int c_in;
      for (i = 0; i < numSplits; i++)
      {
        out = new FileWriter("splits/" + i + ".txt");

        c_in = is.read();
        readerCounter++;
				// if we reach the end of the file, we stop. otherwise, we read until we reach the avarage partition size. then, if we are in the middle of a word, we continue and stop on the next '\n' or ' '
        while (c_in != -1 && ( (readerCounter < avaragePartitionSize + lastSplit) || (c_in != '\n' && c_in != ' ') ))
        {
          out.write((char) c_in);
          c_in = is.read();
          readerCounter++;
        }
        out.close();
        lastSplit = readerCounter;
      }
      is.close();
    } catch (Exception e) {e.printStackTrace();}
  }

  static void runInSlave(String[] command, boolean followingMode)
  {
    BufferedReader br = null;
    AtomicInteger numMachinesFinished = new AtomicInteger(0);
    List<RunInSlave> slaves = new ArrayList<RunInSlave>();
		int requestTimeOut = 300; // = 5 min
    int slaveId = 0; // the id of each slave is given by its line in machines.txt
    String str = null;
    try
    { // we iterate machines.txt and we run a thread for each machine
      br = new BufferedReader(new FileReader("machines.txt"));
      while ((str = br.readLine()) != null)
      {
        slaves.add(new RunInSlave(str, command, numMachinesFinished, slaveId++, followingMode, requestTimeOut));
      }
      br.close();
    } catch (Exception e) {e.printStackTrace();}

		if (followingMode)
			System.out.println("starting machines");
		long startTime = System.nanoTime();

    slaves.forEach( (slave) -> slave.start() );
    slaves.forEach( (slave) -> {try {slave.join();} catch (Exception e) {e.printStackTrace();}} );

		long endTime = System.nanoTime();
		if (followingMode)
			System.out.println("slaves finished after " + ((endTime - startTime) / 1000000) + " ms"); 
		
    if (followingMode)
			System.out.println(numMachinesFinished + " machines finished with ok");
  }

  static int getValidMachines(boolean followingMode)
  {
    try {
      FileWriter out = new FileWriter("machines.txt");
      BufferedReader in = new BufferedReader(new FileReader("rooms.txt"));
      List<ConnexionVerifier[]> tpRooms = new ArrayList<ConnexionVerifier[]>();

			String master = java.net.InetAddress.getLocalHost().getHostName();
      String line = in.readLine();
      int totalNumMachines = 0;
      AtomicInteger validMachines = new AtomicInteger(0);
      while(line != null)
      {
				String[] split = line.split(" ");
				if (split[0].equals("#"))
				{ // room was commented out of the list
					line = in.readLine();
					continue;
				}   
	       
				String room = split[0];
				int numMachines = Integer.parseInt(split[1]);

				ConnexionVerifier array[] = new ConnexionVerifier[numMachines];

				String machineName = null;
				int i;
				for (i = 0; i < numMachines; i++)
				{
					if ((i+1) < 10)
						machineName = "tp-" + room + "-0" + (i+1);
					else
						machineName = "tp-" + room + "-" + (i+1);

					if (machineName.equals(master))
					{
						array[i] = null;
						// skip the Master
						continue;
					}
					totalNumMachines++;
					array[i] = new ConnexionVerifier(machineName, out, validMachines, followingMode);
					array[i].start();
				}
				tpRooms.add(array);

				line = in.readLine();
      }

      ListIterator<ConnexionVerifier[]> it = tpRooms.listIterator();
      while(it.hasNext())
      { 
				int i;
				ConnexionVerifier array[] = it.next();
				for (i = 0; i < array.length; i++)
				{
					try { if (array[i] != null) array[i].join();} catch(Exception e) {e.printStackTrace();}
				}
      }
      in.close();
      out.close();

      if (followingMode)
				System.out.println(validMachines + " of " + totalNumMachines + " machines were validated");

      return validMachines.get();
    } catch (Exception e) {e.printStackTrace();}
    return -1;
  }
}
