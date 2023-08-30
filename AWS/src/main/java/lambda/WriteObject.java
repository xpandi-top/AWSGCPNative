package lambda;

import basic.ObjectStorage;
import basic.Request;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import saaf.Inspector;

import java.io.*;
import java.util.Date;
import java.util.HashMap;

import static basic.MetadataRetriever.*;
import static basic.FileTransform.*;

public class WriteObject implements RequestHandler<Request, HashMap<String,Object>> {
    static AmazonS3 s3Client;
    static BasicAWSCredentials awsCreds = new BasicAWSCredentials(identity, credential);
    private static Long initializeConnectionTime;

    @Override
    public HashMap<String, Object> handleRequest(Request request, Context context) {
        Inspector inspector = new Inspector();
        if (!isMac){
            inspector.inspectAll();
        }
        //****************START FUNCTION IMPLEMENTATION*************************
        // get parameters
        int count = 2;
        int actual_count = 0;
        boolean connect = true;
        if (request!=null&&request.getObjectName()!=null) objectName = request.getObjectName();
        if (request!=null && request.getCount()>0) count = request.getCount();
        if (request != null && request.getContainerName() != null) containerName = request.getContainerName();
        // Initialize s3Client
        if (s3Client==null){
            s3Client = AmazonS3ClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                    .withRegion(Regions.DEFAULT_REGION)
                    .build();
            initializeConnectionTime = new Date().getTime();
        }
        // put object to s3
        try {
            StringWriter sw = new StringWriter();
            fileGenerate(sw,count);
            ObjectStorage.writeBlob(s3Client,containerName,objectName,sw.toString());
        }catch (Exception e){
            inspector.addAttribute("duration", new Date().getTime()-initializeConnectionTime);
            e.printStackTrace();
            connect = false;
        }
        //*********************Function end
        getMetrics(isMac,inspector,count,actual_count,connect,initializeConnectionTime);
        return inspector.finish();
    }
}
