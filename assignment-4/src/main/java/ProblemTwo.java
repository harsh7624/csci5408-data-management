import com.mongodb.client.*;
import org.bson.Document;

import java.io.*;
import java.util.*;


public class ProblemTwo {

    public static List<String> process(HashMap<String,Integer> bagOfWords, List<String> positiveWords, List<String> negativeWords, String tweetText, int counter) throws IOException {

        List<String> matchWords = new ArrayList<>();

        int score = 0;
        for (Map.Entry<String, Integer> entry : bagOfWords.entrySet()) {
            String key = entry.getKey();
            int value = entry.getValue();

            if(positiveWords.contains(key)){
                score =  score + value;
                matchWords.add(key);
            }
            else if(negativeWords.contains(key)){
                score =  score - value;
                matchWords.add(key);
            } else{

            }
        }

        String listString = "";

        for (String s : matchWords)
        {
            listString += s + ", ";
        }

        String polarity = "";
        if(score < 0){
            polarity = " negative ";
        }else if(score > 0){
            polarity = " positive ";
        } else{
            polarity = " neutral ";
        }

        System.out.println(String.format("%10s %4s  %35s %4s  %35s %4s %10s ", counter,"|",tweetText.substring(0, tweetText.length()/2) + System.lineSeparator() + tweetText.substring(tweetText.length()/2),"|", listString,"|", polarity));
        List<String> messages = Arrays.asList(tweetText, listString, polarity);
//        System.out.printf("%-5d  %-100s   %-12s   %-21s%n", counter,tweetText.substring(0, tweetText.length()/2) + System.lineSeparator() + tweetText.substring(tweetText.length()/2), listString, polarity);
        System.out.println("\n");
        return messages;
    }


    public static void main(String[] args) {
        try {

            String connectionString = "mongodb+srv://harshpatel7624:admin@data-assignment-3.azx5p.mongodb.net/rawDb?retryWrites=true&w=majority";
            List<String> positiveString = new ArrayList<>();
            List<String> negativeString = new ArrayList<>();

            BufferedReader br = new BufferedReader(new FileReader("S:\\Dalhousie_course_work\\5408-data\\Assignment-4\\txt-files\\positive-words.txt"));
            BufferedReader br2 = new BufferedReader(new FileReader("S:\\Dalhousie_course_work\\5408-data\\Assignment-4\\txt-files\\negative-words.txt"));


            StringBuilder sb = new StringBuilder();
            String line;

             do {
                line = br.readLine();
                positiveString.add(line);
            }while (line != null);

            br.close();

            String line2 = br2.readLine();
            while (line2 != null) {
                line2 = br2.readLine();
                negativeString.add(line2);
            }

            br2.close();

            FileWriter csvWriter = new FileWriter("problem2.csv");
            csvWriter.append("id");
            csvWriter.append(',');
            csvWriter.append("twitterText");
            csvWriter.append(',');
            csvWriter.append("listString");
            csvWriter.append(',');
            csvWriter.append("polarity");
            csvWriter.append('\n');


            try (MongoClient mongoClient = MongoClients.create(connectionString)) {
                MongoDatabase database = mongoClient.getDatabase("processedDb");
                MongoCollection<Document> processedCollection = database.getCollection("cleanTweets");
                FindIterable<Document> findIterable = processedCollection.find();
                int counter = 0;
                List<String> response = new ArrayList<String>();
                for(Document tweet: findIterable){
                    counter++;
                    String tweetText = tweet.get("text").toString();

                    String[] tweetWords = tweetText.split("\\s+");
                    HashMap<String, Integer> bagOfWords = new HashMap<String,Integer>();
                    for(int i = 0 ; i < tweetWords.length ; i++){
                        tweetWords[i] = tweetWords[i].toLowerCase();
                        if(bagOfWords.get(tweetWords[i]) == null){
                            bagOfWords.put(tweetWords[i],1);
                        }else{
                            bagOfWords.put(tweetWords[i],bagOfWords.get(tweetWords[i]) + 1);
                        }

                    }
                    response = process(bagOfWords,positiveString,negativeString,tweetText,counter);

                    Integer y = new Integer(counter);
                    csvWriter.append(y.toString());
                    csvWriter.append(',');
                    csvWriter.append(response.get(0));
                    csvWriter.append(',');
                    csvWriter.append(response.get(1));
                    csvWriter.append(',');
                    csvWriter.append(response.get(2));
                    csvWriter.append('\n');


                }
                csvWriter.flush();
                csvWriter.close();

            } catch (Exception e){
                System.out.println("Exception occured in mongo client"+e);
            }
        }
        catch (Exception e){

        }


    }
}
