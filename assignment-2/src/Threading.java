import java.io.FileWriter;
import java.sql.*;

public class Threading extends Thread {
    
    String city_name;
    String zip_code;
    String query;
    String priority;
    FileWriter fileWriter;
    Threading(String cityName,String code, String prio,FileWriter fw){
        city_name = cityName;
        zip_code = code;
        priority = prio;
        query = "Update olist_customers_dataset set customer_city ="+ city_name + " where customer_zip_code_prefix="+zip_code;
        fileWriter = fw;
    }
    @Override
    public void run(){
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection con1 = DriverManager.getConnection("jdbc:mysql://34.130.69.82:3306/instance1", "root", "Admin@556");
            con1.setAutoCommit(false);
            fileWriter.write("Line 25: Connection established \n");
            fileWriter.flush();
            Statement st1 = con1.createStatement();
            ResultSet rst1 = st1.executeQuery("SELECT * FROM olist_customers_dataset where customer_zip_code_prefix="+zip_code);
            ResultSetMetaData rsmd = rst1.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            while(rst1.next())
            {
                for(int i = 1; i < columnsNumber; i++)
                    System.out.print(" " + rst1.getString(i) + " ");
                System.out.println(" \n");
            }
            fileWriter.write("Line 30: Select statement executed \n");
            fileWriter.flush();
            int rowsupdated1 = st1.executeUpdate("Update olist_customers_dataset set customer_city='"+city_name + "' where customer_zip_code_prefix="+zip_code);
            System.out.println("rowsupdated1: "+rowsupdated1);
            fileWriter.write("Line 34: Update statement executed \n");
            fileWriter.flush();
            if(priority == "high"){
                sleep(4000);
                con1.commit();
            }else{
                con1.commit();
            }
            
            fileWriter.write("Line 36: Committed Priority : " +priority + "\n" );
            fileWriter.flush();
            con1.close();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        

    }
}
