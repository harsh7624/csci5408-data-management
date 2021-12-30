import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.ArrayList;
import java.util.List;


import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
public class App {
    public static void main(String[] args)   {
//    API key consumerKey:    ocgXasoYpcjDCUm00D6OOmwky
//    consumerSecret API secret:   PNaELR9zssXafJRBrly9ziCfZGw5C4uv0zX4u3dxtYvNH2btoR
//    Bearer Token:    AAAAAAAAAAAAAAAAAAAAAPSsVgEAAAAAzTeMSQcmR56UTzMXbYFEfs6gnr4%3DLu5mrfzSboCs9LXMMJ6iFRb0ZLwfnqanRofn2oDOE2qcyaWAVq
//    Access Token:  1458540078838566921-GeGb1DulidLb5E8WRzOg4NHoMpuaxb
//    Access Token Secret: VyeuqHqUWhV0DJkAYegxB623nxdjxDyOQIA2bvuH6X7qB
//    mongo db mongodb+srv://harshpatel7624:<password>@data-assignment-3.azx5p.mongodb.net/rawDb?retryWrites=true&w=majority
    try {
        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.setDebugEnabled(true).setOAuthConsumerKey("ocgXasoYpcjDCUm00D6OOmwky")
                .setOAuthConsumerSecret("PNaELR9zssXafJRBrly9ziCfZGw5C4uv0zX4u3dxtYvNH2btoR")
                .setOAuthAccessToken("1458540078838566921-GeGb1DulidLb5E8WRzOg4NHoMpuaxb")
                .setOAuthAccessTokenSecret("VyeuqHqUWhV0DJkAYegxB623nxdjxDyOQIA2bvuH6X7qB");

        TwitterFactory twitterFactory = new TwitterFactory(configurationBuilder.build());
        Twitter twitter = twitterFactory.getInstance();
        String[] keywords = new String[]{"weather","hockey","Canada","Temperature","Education"};
        String connectionString = "mongodb+srv://harshpatel7624:admin@data-assignment-3.azx5p.mongodb.net/rawDb?retryWrites=true&w=majority";


        for(int i = 0 ;i < keywords.length ; i++){
                Query loopQuery = new Query(keywords[i]);
                loopQuery.setCount(100);
                QueryResult queryResult;
                int count = 0;
            do {
                if(count < 500) {
                    queryResult = twitter.search(loopQuery);
                    List<Status> tweets = queryResult.getTweets();
                    for (Status tweet : tweets) {
                        count++;
                        try (MongoClient mongoClient = MongoClients.create(connectionString)) {
                            MongoDatabase database = mongoClient.getDatabase("rawDb");
                            MongoCollection<Document> rawDb = database.getCollection("tweets");
                            Thread.sleep(100);
                            Document tweet1 = new Document("id", tweet.getId())
                                    .append("screenName", tweet.getUser().getScreenName())
                                    .append("text", tweet.getText())
                                    .append("createdAt", tweet.getCreatedAt())
                                    .append("userLocation", tweet.getUser().getLocation());

                            rawDb.insertOne(tweet1);
                        }
                        catch (InterruptedException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    }
                }else {
                    break;
                }
            } while ((loopQuery = queryResult.nextQuery()) != null);

        }

    }
    catch (TwitterException t){

    }


    }
}
