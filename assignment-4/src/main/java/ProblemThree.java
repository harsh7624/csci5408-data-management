import com.mongodb.client.*;
import org.bson.Document;

import java.util.*;
import java.util.regex.*;
public class ProblemThree {

    public static void main(String args[]){

        String connectionString = "mongodb+srv://harshpatel7624:admin@data-assignment-3.azx5p.mongodb.net/rawDb?retryWrites=true&w=majority";

        try (MongoClient mongoClient = MongoClients.create(connectionString)) {
            MongoDatabase reuterDatabase = mongoClient.getDatabase("ReuterDb");
            MongoCollection<Document> reuterCollection = reuterDatabase.getCollection("reuter");

            FindIterable<Document> reuterFindIterable = reuterCollection.find();
            List<String> canadaStrings =  new ArrayList<>();
            List<Integer> canadaIndex =  new ArrayList<>();
            List<String> monctonStrings =  new ArrayList<>();
            List<Integer> monctonIndex =  new ArrayList<>();
            List<String> torontoStrings =  new ArrayList<>();
            List<Integer> torontoIndex =  new ArrayList<>();

            int totalCounter = 0;
            int canadaCount = 0;
            int monctonCount = 0;
            int torontoCount = 0;
            for(Document newsArticles: reuterFindIterable){
                totalCounter++;
                String newsBody = newsArticles.get("body").toString();
                String newsBodyToLowercase = newsBody.toLowerCase();

//              GET count on in how many body canada was present.
                if(newsBodyToLowercase.contains("canada")){
                    canadaStrings.add(newsBodyToLowercase);
                    canadaIndex.add(totalCounter);
                    canadaCount++;
                } else if(newsBodyToLowercase.contains("moncton")){
                    monctonStrings.add(newsBodyToLowercase);

                    monctonIndex.add(totalCounter);
                    monctonCount++;
                } else if(newsBodyToLowercase.contains("toronto")){
                    torontoStrings.add(newsBodyToLowercase);

                    torontoIndex.add(totalCounter);
                    torontoCount++;
                }

            }

            double dfCanada = 0;
            double dfMoncton = 0;
            double dfToronto = 0;
            if(canadaCount > 0) {
                dfCanada = totalCounter/canadaCount;
            }
            if(monctonCount > 0) {
                dfMoncton = totalCounter/monctonCount;
            }
            if(torontoCount > 0) {
                dfToronto = totalCounter/torontoCount;
            }
            System.out.println("Total Documents: "+totalCounter);
            System.out.format("%20s%20s%20s%20s\n", "Search Query", "Term", "N/df" , "Log10(N/df)");
            System.out.format("%20s%20s%20s%20s\n", "Canada", canadaCount, "" +totalCounter + "/" + canadaCount + "" , canadaCount > 0 ? Math.log10(totalCounter/dfCanada) : "N.A.");
            System.out.format("%20s%20s%20s%20s\n", "Toronto", torontoCount, "" +totalCounter + "/" + torontoCount + "" , torontoCount > 0 ? Math.log10(totalCounter/dfToronto) : "N.A.");
            System.out.format("%20s%20s%20s%20s\n", "Moncton", monctonCount, "" +totalCounter + "/" + monctonCount + "" , monctonCount > 0 ? Math.log10(totalCounter/dfMoncton) : "N.A.");

            System.out.println("Term: Canada");
            System.out.println("Canada appeared in " + canadaCount + " documents");


            System.out.format("%20s%20s%20s\n", "Article #", "Total Words", "Frequency");
            double highestFrequency = 0;
            double temp = 0;
            String newsArticle = "";
            for(int i = 0; i <  canadaStrings.size() ; i++){
                int countCanada = 0;
                String[] totalWords = canadaStrings.get(i).split("\\s+");
                for(int j = 0 ; j < totalWords.length;j++ ){
                    if(totalWords[j].contains("canada")){
                        countCanada++;
                    }
                }
                temp = ((double) countCanada/totalWords.length);
                if(temp > highestFrequency){
                    highestFrequency = temp;
                    newsArticle = canadaStrings.get(i);
                }
                System.out.format("%20s%20s%20s\n", canadaIndex.get(i), totalWords.length, countCanada);
            }
            System.out.println("newsArticle: " + newsArticle);

        }


    }
}
