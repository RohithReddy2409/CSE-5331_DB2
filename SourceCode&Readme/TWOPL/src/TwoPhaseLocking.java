
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.Scanner;

public class TwoPhaseLocking {

    public static void main(String args[]) {
        try {
            File files = new File("/Users/rohithpeta/Documents/DB2/input3.txt");
            FileInputStream filestream;
            //Reading Input File from the System
            filestream = new FileInputStream(files);
            BufferedReader buffer = new BufferedReader(new InputStreamReader(filestream));
            String inputline = null;
            while ((inputline = buffer.readLine()) != null) {
                //Read Input Line By Line
                inputline = inputline.replaceAll("\\s+","");
                String nextToken = inputline;
                write("\r\n");
                write(nextToken);
                nextToken = nextToken.replaceAll("\\(", "");
                nextToken = nextToken.replaceAll("\\)", "");
                System.out.println(nextToken);
                execute(nextToken);
            }
            buffer.close();
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
    public static void execute(String character) {

        if (character.charAt(0) == 'b') {
            RigorousTransaction.startTransaction(character);
        } else if (character.charAt(0) == 'r') {
            RigorousTransaction.rigorousTwoPhaseReadTransaction(character);
        } else if (character.charAt(0) == 'w') {
            RigorousTransaction.rigorousTwoPhaseWriteTransaction(character);
        } else if (character.charAt(0) == 'e') {
            RigorousTransaction.closeTransaction(character);
        }
    }

    public static void write(String input) {
        try {
            Writer writer;
            writer = new BufferedWriter(new FileWriter("/Users/rohithpeta/Documents/DB2/output3.txt",true));
            writer.append(input);
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}