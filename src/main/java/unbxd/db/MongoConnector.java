package unbxd.db;

import com.mongodb.*;


public class MongoConnector {

    public static MongoClient mongoClient = new MongoClient(
            "ec2-54-251-15-152.ap-southeast-1.compute.amazonaws.com",30000
    );

}
