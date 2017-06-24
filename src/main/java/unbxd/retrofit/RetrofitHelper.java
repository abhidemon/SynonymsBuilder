package unbxd.retrofit;


import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by abhi on 30/03/17.
 */
public class RetrofitHelper {

    static final ObjectMapper mapper = new ObjectMapper();
    
    public static <S> S createService(Retrofit.Builder builder ,Class<S> serviceClass) {
        Retrofit retrofit = builder.build();
        return retrofit.create(serviceClass);
    }

    public static <S> S createService(String baseUrl ,Class<S> serviceClass) {
        Retrofit.Builder builder = loadBuilder(baseUrl);
        Retrofit retrofit = builder.build();
        return retrofit.create(serviceClass);
    }

    public static <S> S createService(String baseUrl , Map<String,String> headers ,Class<S> serviceClass) {
        Retrofit.Builder builder = loadBuilder( baseUrl, headers );
        Retrofit retrofit = builder.build();
        return retrofit.create(serviceClass);
    }

    public static MultipartBody.Part getPartFromFileName(String relFileName) throws IOException {
        return getPartFromFile(new File(relFileName));
    }

    private static OkHttpClient.Builder builder = new OkHttpClient.Builder()
            .retryOnConnectionFailure(true)
            .connectTimeout(10, TimeUnit.SECONDS)
            .readTimeout(10, TimeUnit.SECONDS);


    private static OkHttpClient client = builder
            .build();


    public static Retrofit.Builder loadBuilder( String baseUrl  ){
        return new Retrofit.Builder()
                .baseUrl( baseUrl )
                .addConverterFactory( JacksonConverterFactory.create() )
                .client( client );
    }

    public static Retrofit.Builder loadBuilder( String baseUrl  , Map<String,String> headers  ){
        return new Retrofit.Builder()
                .baseUrl( baseUrl )
                .addConverterFactory( JacksonConverterFactory.create() )
                .client( loadOkHttpClient(headers) );
    }


    private static OkHttpClient loadOkHttpClient( Map<String,String> headers  ) {
        builder.addInterceptor(chain -> {
            Request.Builder reqBuilder = chain
                    .request()
                    .newBuilder();
            headers.forEach( (key, value) -> reqBuilder.addHeader(key, value) );
            return chain.proceed( reqBuilder.build() );
        });
        return builder.build();
    }


    public static MultipartBody.Part getPartFromFile(File file) {
        RequestBody requestFile =
                RequestBody.create(MediaType.parse("multipart/form-data"), file);
        MultipartBody.Part body =
                MultipartBody.Part.createFormData("file", file.getName(), requestFile);
        return body;

    }

    public static HashMap<String,Object> getResponseBodyMap(retrofit2.Response<ResponseBody> response) throws IOException {
        return mapper.readValue(new String(response.body().bytes()),HashMap.class);
    }



}
