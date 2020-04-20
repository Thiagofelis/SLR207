import java.util.*;
import java.io.*;


public class WordCounter
{
  private static List<Map.Entry<String, Integer>> sort(HashMap<String, Integer> map)
  {
     List<Map.Entry<String, Integer>> list = new LinkedList<Map.Entry<String, Integer>>(map.entrySet());

     Collections.sort(list, new Comparator<Map.Entry<String, Integer>>()
     {
        public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2)
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
        }
     } );

     return list;
  }

  static void printMap(List<Map.Entry<String, Integer>> list, PrintWriter writer)
  {
    Iterator<Map.Entry<String, Integer>> itr = list.iterator();

    while(itr.hasNext())
    {
      Map.Entry<String, Integer> entry = itr.next();
      writer.println(entry.getValue() + " " + entry.getKey());
    }
  }


  public static void main(String[] args) throws Exception
  {
    HashMap<String, Integer> WordMap = new HashMap<String, Integer>();
    File file = new File(args[0] + ".txt");

    BufferedReader br = new BufferedReader(new FileReader(file));
    int i;
    String str;
    String[] splited = null;

    // counts number of occurences
    long startTime = System.currentTimeMillis();
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
        if (WordMap.containsKey(splited[i]))
        {
          WordMap.put(splited[i], WordMap.get(splited[i]) + 1);
        }
        else
        {
          WordMap.put(splited[i], 1);
        }
      }
    }
    long countTime = System.currentTimeMillis() - startTime;

    PrintWriter writer = new PrintWriter (new BufferedWriter(new FileWriter(args[0] + "_out.txt")));

    startTime = System.currentTimeMillis();
    List<Map.Entry<String, Integer>> list = sort(WordMap);
    long sortTime = System.currentTimeMillis() - startTime;

    printMap(list, writer);
    System.out.println("countTime: " + countTime + "\nsortTime: " + sortTime);
    writer.close();

  }
}
