import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ProblemTwo {

    public static void main(String[] args) throws FileNotFoundException {
        String REUTERS_1_PATH = "S:\\assignment-3\\sgm-files\\reut2-009.sgm";
        String REUTERS_2_PATH = "S:\\assignment-3\\sgm-files\\reut2-014.sgm";
        BufferedReader bufferedReader = new BufferedReader(new FileReader(REUTERS_1_PATH));

        try {
            StringBuilder stringBuilder = new StringBuilder();

            String line = bufferedReader.readLine();

            while (line != null) {
                stringBuilder.append(line);
                stringBuilder.append("\n");
                line = bufferedReader.readLine();
            }
            String Reuters1 = stringBuilder.toString();

            Thread.sleep(1000);
            String[] stringBetweenReuters = getStringBetweenTags(Reuters1, "REUTERS");;
            List<Document> reutersData = new ArrayList<>();
            for(int i = 0; i < stringBetweenReuters.length;i++){
                String[] title = getStringBetweenTags(stringBetweenReuters[i], "TITLE");
                String[] dateLine = getStringBetweenTags(stringBetweenReuters[i], "DATELINE");
                String[] body = getStringBetweenTags(stringBetweenReuters[i], "BODY");

                if(body.length < 1 || dateLine.length < 1 || title.length < 1){
                    continue;
                }

                reutersData.add(new Document("title", title[0])
                        .append("dateLine", dateLine.length > 0 ? dateLine[0] : null)
                        .append("body", body.length > 0 ? body[0] : null));
            }

            BufferedReader bufferedReader2 = new BufferedReader(new FileReader(REUTERS_2_PATH));
            StringBuilder stringBuilder2 = new StringBuilder();

            String line2 = bufferedReader2.readLine();

            while (line2 != null) {
                stringBuilder2.append(line2);
                stringBuilder2.append("\n");
                line2 = bufferedReader2.readLine();
            }
            String Reuters2 = stringBuilder2.toString();

            Thread.sleep(1000);
            String[] stringBetweenReuters2 = getStringBetweenTags(Reuters2, "REUTERS");

            for(int i = 0; i < stringBetweenReuters2.length;i++){
                String[] title = getStringBetweenTags(stringBetweenReuters2[i], "TITLE");
                String[] dateLine = getStringBetweenTags(stringBetweenReuters2[i], "DATELINE");
                String[] body = getStringBetweenTags(stringBetweenReuters2[i], "BODY");
                if(body.length < 1 || dateLine.length < 1 || title.length < 1){
                    continue;
                }
                System.out.println("title: " +title);
                System.out.println("dateLine: " +dateLine);

                reutersData.add(new Document("title", title[0])
                        .append("dateLine", dateLine[0])
                        .append("body", body[0]));

            }
            String connectionString = "mongodb+srv://harshpatel7624:admin@data-assignment-3.azx5p.mongodb.net/rawDb?retryWrites=true&w=majority";
            try (MongoClient mongoClient = MongoClients.create(connectionString)) {
                MongoDatabase reuterDb = mongoClient.getDatabase("ReuterDb");
                MongoCollection<Document> reuterDbCollection = reuterDb.getCollection("reuter");
                reuterDbCollection.insertMany(reutersData);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String[] getStringBetweenTags(String inputString, String tag) {
        List<String> list = new ArrayList<>();
        String pattern = "<" + tag + "[^>]*>(.*?)</" + tag + ">";
        Pattern r = Pattern.compile(pattern, Pattern.DOTALL);
        Matcher matcher = r.matcher(inputString);
        while (matcher.find()) {
            String match = matcher.group(1);
            list.add(match);
        }
        return list.toArray(new String[list.size()]);
    }
}