import com.mongodb.client.*;
import org.bson.Document;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

import java.util.ArrayList;
import java.util.List;

public class CleanData {

    public static void main(String[] args){


        try {
            ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
            configurationBuilder.setDebugEnabled(true).setOAuthConsumerKey("ocgXasoYpcjDCUm00D6OOmwky")
                    .setOAuthConsumerSecret("PNaELR9zssXafJRBrly9ziCfZGw5C4uv0zX4u3dxtYvNH2btoR")
                    .setOAuthAccessToken("1458540078838566921-GeGb1DulidLb5E8WRzOg4NHoMpuaxb")
                    .setOAuthAccessTokenSecret("VyeuqHqUWhV0DJkAYegxB623nxdjxDyOQIA2bvuH6X7qB");

            TwitterFactory twitterFactory = new TwitterFactory(configurationBuilder.build());
            Twitter twitter = twitterFactory.getInstance();
            String connectionString = "mongodb+srv://harshpatel7624:admin@data-assignment-3.azx5p.mongodb.net/rawDb?retryWrites=true&w=majority";

            try (MongoClient mongoClient = MongoClients.create(connectionString)) {
                List<Document> databases = mongoClient.listDatabases().into(new ArrayList<>());
                databases.forEach(db -> System.out.println(db.toJson()));
                MongoDatabase database = mongoClient.getDatabase("rawDb");
                MongoCollection<Document> rawDbCollection = database.getCollection("tweets");
                FindIterable<Document> findIterable = rawDbCollection.find();
                List<Document> tweetData = new ArrayList<>();

                List<Document> processedTweet = new ArrayList<>();
                for(Document tweet: findIterable){
                    System.out.println("tweet:" + tweet);
                    String tweetText = tweet.get("text").toString();

                    String emojiRemoved = tweetText.replaceAll("(\\u00a9|\\u00ae|[\\u2000-\\u3300]|\\ud83c[\\ud000-\\udfff]|\\ud83d[\\ud000-\\udfff]|\\ud83e[\\ud000-\\udfff])","");

                    String removeURL = emojiRemoved.replaceAll("(http|ftp|https):(\\\\\\/\\\\\\/|\\/\\/)([\\w_-]+(?:(?:\\.[\\w_-]+)+))([\\w.,@?^=%&:\\\\\\/~+#-]*[\\w@?^=%&\\/~+#-])","");

                    System.out.println("removeURL" + removeURL);
                    tweet.put("text", removeURL);
                    processedTweet.add(tweet);
                }
                MongoDatabase processedDatabase = mongoClient.getDatabase("processedDb");
                MongoCollection<Document> processedDbCollection = processedDatabase.getCollection("cleanTweets");
                processedDbCollection.insertMany(processedTweet);
            }catch (Exception e){
                System.out.println("Exception occured in mongo client"+e);
            }
        }
        catch (Exception e){

        }
    }
}
