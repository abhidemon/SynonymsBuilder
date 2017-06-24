package unbxd.crons;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import okhttp3.ResponseBody;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import retrofit2.Call;
import retrofit2.Response;
import retrofit2.http.*;
import unbxd.db.RedisConnector;
import unbxd.retrofit.RetrofitHelper;
import unbxd.util.Tup2;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static unbxd.crons.WordRelationsBuilderCron.MAXRETRIES_FORCONCEPTNET;
import static unbxd.crons.WordRelationsBuilderCron.WAIT_MS_BEFORE_RETRY;

/**
 * Created by abhi on 30/03/17.
 */

public class ConceptNetExtracter {

    private static AtomicInteger reqCount = new AtomicInteger();

    private static final Logger LOGGER = LogManager.getLogger(ConceptNetExtracter.class);

    static final String conceptNetUrl = "http://api.conceptnet.unbxdapi.com";
    //static final String conceptNetUrl = "http://api.conceptnet.io";
    static ObjectMapper MAPPER = new ObjectMapper();
    static final Class<? extends Map> MAP_CLAZZ = new HashMap<String, Object>().getClass();

    static Map<String,Object> synParamsMap = new HashMap<>();
        static {
            synParamsMap.put("rel", "/r/Synonym");
            synParamsMap.put("language", "en");
            synParamsMap.put("limit", 1000);
        }

    static Map<String,Object> formOFParamsMap = new HashMap<>();
    static {
        formOFParamsMap.put("rel", "/r/FormOf");
        formOFParamsMap.put("language", "en");
        formOFParamsMap.put("limit", 1000);
    }

    static Map<String,String> header = new HashMap<>();
    static {
        header.put("Content-Type","application/json");
    }

    private String token;

    public ConceptNetExtracter(String token) {
        this.token = token;
    }

    static final String accepts = "*/*";

    public interface ConceptNetService {
        @GET
        Call<ResponseBody> get(@Header("Accept") String contentType,
                               @Url String url, @QueryMap Map<String, Object> params);
    }


    static ConceptNetService service = RetrofitHelper.createService(conceptNetUrl, header ,ConceptNetService.class);

    public static Set<String> getConceptNetSuggs(String token,Map<String,Object> paramsMap) throws IOException {
        Set<String> suggs = new HashSet<>();
        while (reqCount.get()>300){
            LOGGER.info("Too many request, waiting...");
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        try{
            reqCount.incrementAndGet();
            suggs = getConceptNetSuggs(token, paramsMap, MAXRETRIES_FORCONCEPTNET);
        }catch (Exception e){
            LOGGER.error(e.getMessage());
            throw e;
        }finally {
            reqCount.decrementAndGet();
        }
        return suggs;
    }
    public static Set<String> getConceptNetSuggs(String token,Map<String,Object> paramsMap, int tries) throws IOException {

        Response<ResponseBody> call = service.get(accepts,"query?node=/c/en/" + token, paramsMap).execute();

        String body=null;
        if (call.body()!=null){
            body = new String(call.body().bytes());
        }else if (call.errorBody()!=null){
            body = new String(call.errorBody().bytes());
            body = call.raw().toString() + "__" +body ;
        }else{
            body = "{\n" +
                    "  \"@context\": [\n" +
                    "    \"http://api.conceptnet.io/ld/conceptnet5.5/context.ld.json\",\n" +
                    "    \"http://api.conceptnet.io/ld/conceptnet5.5/pagination.ld.json\"\n" +
                    "  ],\n" +
                    "  \"@id\": \"/query?rel=/r/FormOf&node=/c/en/Interior\",\n" +
                    "  \"edges\": []\n" +
                    "}";
        }
        LOGGER.debug(body);
        Set<String> terms = new HashSet<String>();
        Map<String,Object> response = null;
        try{
            response = MAPPER.readValue(body, MAP_CLAZZ);
        }catch (Exception e){
            if (tries>=0 ){
                LOGGER.error("Retrying for error: "+e.getMessage());
                try {
                    Thread.sleep(WAIT_MS_BEFORE_RETRY);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
                return getConceptNetSuggs(token,paramsMap,--tries);
            }else{
                LOGGER.error(e.getMessage());
                return terms;
            }
        }
        List<Map<String,Object>> edges = (List<Map<String,Object>>)response.get("edges");
        for (Map<String,Object> edge : edges ) {
            if  ( !  ( (Map<String,Object>)edge.get("rel")).get("@id").equals(paramsMap.get("rel"))   ){
                LOGGER.error("Anomaly.!!!!"+edge);
            }else{
                Map<String, Object> start = (Map<String, Object>) edge.get("start");
                if ( start.get("language").equals("en")){
                    String sugg = String.valueOf(start.get("label"));
                    if ( ! sugg.trim().equals("") ){
                        terms.add( sugg );
                    }
                }
            }
        }
        terms.remove(token);
        return terms;

    }

    public Set<String> getSynonyms() throws IOException {
        return getConceptNetSuggs(token,synParamsMap);
    }

    public List<String> getRelatedTerms()  {
        try {
            String relatedTerms=null;
            try{
                relatedTerms = RedisConnector.get(token);
            }catch (NoClassDefFoundError e){
                LOGGER.error(e.getMessage());
            }catch (Exception e){
                LOGGER.error(e.getMessage());
            }
            if ( relatedTerms != null ){
                relatedTerms = relatedTerms.trim();
                if ( relatedTerms.equals("") ){
                    RedisConnector.del(token);
                }else{
                    return Arrays.asList( relatedTerms.split(",") );
                }

            }
            ArrayList<String> relWrds = new ArrayList<>(getConceptNetSuggs(token, formOFParamsMap));
            try{
                RedisConnector.set(token, Joiner.on(",").join( relWrds ));
            }catch (Exception e){
                LOGGER.warn(e.getMessage());
            }
            return relWrds;
        } catch (IOException e) {
            e.printStackTrace();
            return new ArrayList<>();
        }
    }

    public Tup2<String,List<String>> getRelatedTermsTuple()  {
        return new Tup2<>(token,getRelatedTerms());
    }


    public static void main1() throws IOException {

        String[] tokens = { "cereal","power", "dog", "bread", "meat" };
        for (String token : tokens){
            //Set<String> tokensW = getSynonyms(token);
            //System.out.println(token+" -> "+tokensW);
            List<String> tokensF = new ConceptNetExtracter(token).getRelatedTerms();
            System.out.println(token+" -> "+tokensF);

        }
    }

}
