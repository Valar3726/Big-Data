package createTransactions;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
public class createTransactions {

	public static String getRandomString(int length) {
	    String base = "abcdefghijklmnopqrstuvwxyz";
	    Random random = new Random();
	    StringBuffer sb = new StringBuffer();
	    for (int i = 0; i < length; i++) {
	        int number = random.nextInt(base.length());
	        sb.append(base.charAt(number));
	    }
	    return sb.toString();
	 }
	public static int getRandomNumber(int lowbound, int upbound)
	{
		int x=(int)(Math.random()*(upbound-lowbound));
		x = x + lowbound;
		return x;
	}
	public static double getRandomdoubleNumber(int lowbound, int upbound)
	{
		double x= Math.random()*(upbound-lowbound);
		x = x + lowbound;
		return x;
	}

	public static void main(String[] args) {

		 try {
			   FileWriter fw = new FileWriter("/Users/valar/Desktop/Transactions.csv");


			   for (int i = 1; i < 5000001; i++) {
			    StringBuffer str = new StringBuffer();

			     str.append(i+","+getRandomNumber(1,50000)+","+getRandomdoubleNumber(10,1000)+","+getRandomNumber(1,10)+","+getRandomString(getRandomNumber(20,50))+"\r\n");

			    fw.write(str.toString());
			    fw.flush();
			   }
			   fw.close();
			  } catch (IOException e) {
			   e.printStackTrace();
			  }

	}

}
