import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

// Creates sets of X,Y integer coordinates

public class createPoints {

	
	public static int getRandomNumber(int min, int max)
	{
		int range = (max - min) + 1;
		return (int)(Math.random()*range) + min;
	}


	public static void main(String[] args) {

		int min = 0;
		int max = 9999;

		try {
			FileWriter fw = new FileWriter("./points.csv");


			for (int i = 1; i < 10000001; i++) {
				StringBuffer str = new StringBuffer();

				str.append(getRandomNumber(min,max)+","+getRandomNumber(min,max)+"\r\n");

				fw.write(str.toString());
				fw.flush();
			}
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}


}
