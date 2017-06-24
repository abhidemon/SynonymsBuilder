package unbxd.sc;

import com.google.common.io.Files;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by abhi on 31/03/17.
 */
public class SendFileUtilTest {

    SendFileUtil sendFileUtil;


    @Before
    public void setUp() throws Exception {
        sendFileUtil = new SendFileUtil("now_2016_flwsellr_com-u1480588632534","/tmp/wordForms_38065.txt");
    }

    @Test
    public void sendFile() throws Exception {
        sendFileUtil.sendFile();
    }

    @Test
    public void showConfigTest() throws IOException {
        String str = sendFileUtil.getConfigFiles();

    }

    @Test
    public void getServerAddress() throws Exception {
        List<String> addrs = sendFileUtil.getServerAddress();
        System.out.println(addrs);
        List<String> expectedAddrs = Arrays.asList( "http://54.172.116.26:8086", "http://54.89.224.197:8086" );
        Assert.assertTrue( expectedAddrs.containsAll(addrs) && addrs.containsAll(expectedAddrs) );
    }

    @Test
    public void testZipFile2() throws IOException {
        File tempDir = Files.createTempDir();
        String tmpFileName = tempDir.getAbsolutePath() + "/" + System.currentTimeMillis() + ".txt";
        FileOutputStream os = new FileOutputStream(new File(tmpFileName));
        os.write("Line 1 : This is a test entry. ".getBytes());
        os.write("Line 2 : This is also a test entry. And the ending line. ".getBytes());
        os.close();
        String zipFileName = SendFileUtil.zipFile(tmpFileName);
        Assert.assertEquals(zipFileName, tmpFileName+".zip");

    }

}