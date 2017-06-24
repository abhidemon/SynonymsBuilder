package unbxd.sc;

import okhttp3.MultipartBody;
import okhttp3.ResponseBody;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import retrofit2.Call;
import retrofit2.Response;
import retrofit2.http.*;
import unbxd.crons.WordRelationsBuilderCron;
import unbxd.retrofit.RetrofitHelper;
import unbxd.util.Constants;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static unbxd.util.Constants.serverAddressApiUrl;

/**
 * Created by abhi on 31/03/17.
 */
public class SendFileUtil {

    private static final Logger LOGGER = LogManager.getLogger(SendFileUtil.class);

    static final String AUTH_KEY = "Basic dW5ieGQ6CmdrTCVGfmUlNWt3QUpL";
    static ConfigFilesApi configFilesApiService = RetrofitHelper.createService(serverAddressApiUrl+"/", ConfigFilesApi.class);
    static ServerAddress serverAddressService = RetrofitHelper.createService(serverAddressApiUrl+"/", ServerAddress.class);

    String siteKey;
    String fileName;

    public SendFileUtil(String siteKey, String fileName) {
        this.siteKey = siteKey;
        this.fileName = fileName;
    }


    public static String zipFile(String srcPath) throws IOException {
        String destPath  = srcPath+".zip";
        FileOutputStream fos = new FileOutputStream(destPath);
        ZipOutputStream zos = new ZipOutputStream(fos);
        File srcFile = new File(srcPath);
        FileInputStream fis = new FileInputStream(srcFile);
        String basePath = srcFile.getParent() + "/";
        byte[] buffer = new byte[1024];
        zos.putNextEntry(new ZipEntry(srcPath.substring(basePath.length())));
        int length;
        while ((length = fis.read(buffer)) > 0) {
            zos.write(buffer, 0, length);
        }
        zos.closeEntry();
        fis.close();
        zos.close();
        return destPath;
    }

    public boolean sendFile() throws IOException {
        List<String> serverAddressList = getServerAddress();
        String zipFileName = zipFile(fileName);
        MultipartBody.Part file = RetrofitHelper.getPartFromFileName(zipFileName);
        for (String serverAddress : serverAddressList){
            LOGGER.info("Sending "+fileName+" for "+siteKey+" to "+serverAddress);
            Call<ResponseBody> call = configFilesApiService.upload(serverAddress + "/sc/configFiles", siteKey, file, AUTH_KEY);
            Response<ResponseBody> resp = call.execute();
            if (resp.errorBody()!=null){
                throw new IOException(String.valueOf(resp.errorBody()));
            }else{
                String body = new String(resp.body().bytes());
                LOGGER.debug(body);
            }
        }
        return true;
    }


    public interface ConfigFilesApi {
        @Multipart
        @POST
        Call<ResponseBody> upload(
                @Url String url,
                @Query("siteKey") String siteKey,
                @Part MultipartBody.Part file,
                @Header("Authorization") String authKey
        );

        @GET
        Call<ResponseBody> get(
                                @Url String url,
                                @Query("siteKey") String siteKey,
                                @Header("Authorization") String authKey
                               );
    }


    public interface ServerAddress {
        @GET
        Call<ResponseBody> getServerAddress(@Url String url,@Query("iSiteName") String siteKey);
    }

    public List<String> getServerAddress() throws IOException {
        String resp = null;
        try{
            Call<ResponseBody> callR = serverAddressService.getServerAddress(serverAddressApiUrl, siteKey);
            Response<ResponseBody> call = callR.execute();
            if (call.body()!=null){
                resp = new String(call.body().bytes());
            } else if (call.errorBody()!=null) {
                String body = new String(call.errorBody().bytes());
                body = call.raw().toString() + "__" + call;
            }

            }catch (Exception e){
            e.printStackTrace();
        }
        if (resp!=null)
            return Arrays.asList(resp.split(","));
        else
            return new ArrayList<>();
    }

    public String getConfigFiles() throws IOException {
        List<String> serverAddressServices = getServerAddress();
        String string = "ERROR";
        for (String serverAddressService : serverAddressServices){
            Response<ResponseBody> dsdsds = configFilesApiService.get(serverAddressService+"/sc/configFiles", siteKey, AUTH_KEY).execute();
            string = new String(dsdsds.body().bytes());
            break;
        }
        return string;
    }

}
