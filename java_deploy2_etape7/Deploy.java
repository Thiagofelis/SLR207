//package DeployPack;
import java.io.FileWriter;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.concurrent.*;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.atomic.AtomicInteger;

public class Deploy
{
	public static void main(String[] args) throws Exception
	{
    //File myObj = new File("existing_machines.txt");
    //Scanner myReader = new Scanner(myObj);
    FileWriter out = new FileWriter("valid_machines.txt");
		BufferedReader in = new BufferedReader(new FileReader("existing_machines.txt"));
		List<ConnexionVerifier[]> myList = new ArrayList<ConnexionVerifier[]>();

		String line = in.readLine();
		int totalNumMachines = 0;
		AtomicInteger validMachines = new AtomicInteger(0);
		while(line != null)
		{
			String[] split = line.split(" ");
			String room = split[0];
			int numMachines = Integer.parseInt(split[1]);

			ConnexionVerifier array[] = new ConnexionVerifier[numMachines];

			int i;
			String roomIp = "";
			for (i = 0; i < numMachines; i++)
			{
				totalNumMachines++;
				if ((i+1) < 10) array[i] = new ConnexionVerifier("tp-" + room + "-0" + (i+1), out, validMachines);
				else						array[i] = new ConnexionVerifier("tp-" + room + "-" + (i+1), out, validMachines);
				array[i].start();
			}
			myList.add(array);

			line = in.readLine();
		}

		ListIterator<ConnexionVerifier[]> it = myList.listIterator();

		while(it.hasNext())
		{
			int i;
			ConnexionVerifier array[] = it.next();
			for (i = 0; i < array.length; i++)
			{
				try { array[i].join(); } catch(Exception e) {}
			}
		}
		in.close();
		out.close();
		System.out.println(validMachines + " / " + totalNumMachines);
/*
    ConnexionVerifier cv1 = new ConnexionVerifier ("tp-1a222-05", out);
		ConnexionVerifier cv2 = new ConnexionVerifier ("tp-1a222-04", out);
		ConnexionVerifier cv3 = new ConnexionVerifier ("tp-1a222-03", out);
		ConnexionVerifier cv4 = new ConnexionVerifier ("tp-1a222-02", out);
    cv1.start(); cv2.start(); cv3.start(); cv4.start();
    try { cv1.join(); } catch(Exception e) {}
		try { cv2.join(); } catch(Exception e) {}
		try { cv3.join(); } catch(Exception e) {}
		try { cv4.join(); } catch(Exception e) {}*/
	}
}
