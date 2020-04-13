
public class Slave
{
	public static void main(String[] args)
	{
		try
		{
			Thread.sleep(Integer.parseInt(args[0]));
			// args[0] => time to calculate
		}
		catch(InterruptedException ex)
		{
			Thread.currentThread().interrupt();
		}
		if (Integer.parseInt(args[1]) > 0)
		// if args[1] > 0, an error happens 
		{
			System.err.println("erreu");
			return;
		}
		System.out.println(3 + 2);
	}
}
