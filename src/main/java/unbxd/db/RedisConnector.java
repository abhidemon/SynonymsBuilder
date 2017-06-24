package unbxd.db;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.RedisURI;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import unbxd.crons.ConceptNetExtracter;

/**
 * Created by abhi on 31/03/17.
 */
public class RedisConnector {

    private static final Logger LOGGER = LogManager.getLogger(RedisConnector.class);

    public static boolean USE_REDIS = false;

    private static RedisClient redisClient;
    private static RedisConnection<String, String> redisConn;

    static{
        if (USE_REDIS){
            redisClient = new RedisClient(
                    RedisURI.create("redis://54.172.116.26:6969")
            );
            RedisConnection<String, String> connection = redisClient.connect();
            try{
                //Testing connection first.
                connection.get("test");
            }catch (Exception e){
                LOGGER.error(e);
            }
            System.out.println("Connected to Redis");
            LOGGER.info("Connected to Redis");
            redisConn = null;
        }
    }

    public static String get(String key){
        if (redisConn!=null){
            try{
                return redisConn.get(key);
            }catch (Exception e){
                LOGGER.error(e.getMessage());
            }
        }
        return null;
    }

    public static void set(String key, String value){
        if (redisConn!=null){
            try{
                redisConn.get(key);
            }catch (Exception e){
                LOGGER.error(e.getMessage());
            }
        }
    }

    public static void del(String key){
        if (redisConn!=null){
            try{
                redisConn.del(key);
            }catch (Exception e){
                LOGGER.error(e.getMessage());
            }
        }
    }

}
