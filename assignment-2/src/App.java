import java.io.FileWriter;
import java.sql.*;

public class App {
    public static void main(String[] args) throws Exception {
        FileWriter fileWriter = new FileWriter("Logs.txt");
        FileWriter fileWriter2 = new FileWriter("Logs2.txt");
        Threading Thread1 = new Threading("T1 city", "31060","low",fileWriter);
        Threading Thread2 = new Threading("T2 city", "31060","high",fileWriter);
        Threading Thread3 = new Threading("T3 city", "31060","medium",fileWriter);
        
        ThreadingLocks LThread1 = new ThreadingLocks("T1 city", "31060","low",fileWriter2);
        ThreadingLocks LThread2 = new ThreadingLocks("T2 city", "31060","high",fileWriter2);
        ThreadingLocks LThread3 = new ThreadingLocks("T3 city", "31060","medium",fileWriter2);


        // Thread1.start();
        // Thread2.start();
        // Thread3.start();

        LThread1.start();
        LThread2.start();
        LThread3.start();
        
    }

}
