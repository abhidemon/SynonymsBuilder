package unbxd.crons;

import com.google.common.base.Joiner;
import com.google.common.io.Files;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.slf4j.Log4jLogger;
import org.bson.Document;
import unbxd.db.MongoConnector;
import unbxd.sc.SendFileUtil;
import unbxd.util.Constants;
import unbxd.util.Tup2;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Created by abhi on 30/03/17.
 */
public class WordRelationsBuilderCron {

    private static final Logger LOGGER = LogManager.getLogger(WordRelationsBuilderCron.class);

    public static int MAXRETRIES_FORCONCEPTNET = 10;
    public static long WAIT_MS_BEFORE_RETRY = 50;



    public static final String UNBXD_DB_NAME= "UnbxdDB";
    public static final String SITE_INFO_COLL_NMAME = "siteInfo";
    public static final String CLUSTER_INFO = "clusterInfo";
    public static final String CATALOG = "catalog";


    public void scanSites(){
        MongoCursor<Document> siteInfos = MongoConnector.mongoClient
                .getDatabase(UNBXD_DB_NAME)
                .getCollection(SITE_INFO_COLL_NMAME)
                .find()
                .iterator();
        if ( siteInfos.hasNext() ) {

            Document siteInfo = siteInfos.next();
            String siteId = String.valueOf(siteInfo.get("siteId"));
        }
    }

    public static String extractCatalog(String siteID) throws IOException {
        Set<String> relatedTermsCatalog = new HashSet<>();
        File tempDir = Files.createTempDir();

        //String tmpFileName = tempDir.getAbsolutePath() + "/" + "wordForms_"+siteID + ".txt";
        String tempBaseDir = tempDir.getAbsolutePath() + "/" + siteID;
        File tmpFolder = new File(tempBaseDir);
        if ( !tmpFolder.exists() ){
            tmpFolder.mkdirs();
        }
        String tmpFileName = tempBaseDir +"/wordForms.txt";
        LOGGER.info("This will be flushed to file : "+tmpFileName+" , for siteId: "+siteID);
        Writer fileWriter = new FileWriter(tmpFileName);
        MongoCursor<Document> catalog = MongoConnector.mongoClient
                .getDatabase(siteID)
                .getCollection(CATALOG)
                .find()
                .projection(new Document("_id",false))
                .iterator();
        List<CompletableFuture<Tup2<String,List<String>>>> futureList = new ArrayList<>();
        int cnt = 0;
        while (catalog.hasNext()){
            Document entry = catalog.next();
            LOGGER.info("[ " +System.currentTimeMillis()+"] Catalog No. : " + cnt++ );
            entry.forEach((key,value)->{
                HashSet<String> tokens = new HashSet<>(Arrays.asList(String.valueOf(value).split(" ")));
                for (String token : tokens){
                    token = token.replaceAll("[^a-zA-Z0-9]","").toLowerCase();
                    if (!relatedTermsCatalog.contains(token)){
                        CompletableFuture<Tup2<String,List<String>>> future = CompletableFuture.supplyAsync(
                                new ConceptNetExtracter( token )::getRelatedTermsTuple
                        );
                        relatedTermsCatalog.add(token);
                        futureList.add(future);
                    }
                }
                try {
                    captureFutures(futureList,fileWriter,false);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
        captureFutures(futureList,fileWriter,true);
        fileWriter.close();
        //return relatedTermsCatalog;
        return tmpFileName;
    }

    private static void captureFutures(List<CompletableFuture<Tup2<String, List<String>>>> futureList,
                                Writer fileWriter,
                                boolean finishIt) throws IOException {

        if ( futureList.size() > Constants.concurrency | finishIt){
            LOGGER.info(" Checking futures. ");
            futureList.forEach(
                    future -> {
                        try {
                            Tup2<String, List<String>> response = future.get();
                            if (response._2.size()>0){
                                fileWriter.write(response._1+"\t"+ Joiner.on(",").join(response._2) +"\n");
                            }
                            LOGGER.info(response._1+" -> "+response._2);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (ExecutionException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
            );
            futureList.clear();
            fileWriter.flush();
        }
    }


    public static Document getSiteDetails(long siteId){
        MongoCursor<Document> siteObjectItr = MongoConnector.mongoClient
                .getDatabase("FeedDB")
                .getCollection("siteInfo")
                .find(new Document("siteId", siteId)).iterator();
        if ( siteObjectItr.hasNext() ){
            Document siteObj = siteObjectItr.next();
            return siteObj;
        }
        return null;
    }


    public static void main(String... args) throws IOException {
        if (args.length==0){
            System.err.println("Please specify siteId as arg1.");
            System.exit(1);
        }
        if (args.length>1){
            MAXRETRIES_FORCONCEPTNET = Integer.parseInt(args[1]);
        }
        if (args.length>2){
            WAIT_MS_BEFORE_RETRY = Long.parseLong(args[2]);
        }
        String siteId = args[0];
        long startTs = System.currentTimeMillis();
        String fileName = extractCatalog(siteId);
        long endTs = System.currentTimeMillis();
        LOGGER.info("For siteid : "+siteId+" , Total Time taken for extracting data: " + (endTs-startTs) );
        Document site = getSiteDetails(Long.parseLong(siteId));
        String siteKey = String.valueOf(site.get("siteNameInternal"));
        SendFileUtil sendFileUtil = new SendFileUtil(siteKey, fileName);
        sendFileUtil.sendFile();
        LOGGER.info("For siteId:"+siteId+" Total time taken to send file = "+(System.currentTimeMillis()-endTs));
    }


}
