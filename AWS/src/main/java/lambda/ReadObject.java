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

import java.util.Date;
import java.util.HashMap;

import static basic.MetadataRetriever.*;

public class ReadObject implements RequestHandler<Request, HashMap<String,Object>> {
    static AmazonS3 s3Client;
    static BasicAWSCredentials awsCred = new BasicAWSCredentials(identity, credential);
    private static Long initializeConnectionTime;
    public HashMap<String, Object> procedure(Request request, Context context){
        Inspector inspector = new Inspector();
        if (!isMac){
            inspector.inspectAll();
        }
        //*************FunctionStart**************
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
                    .withCredentials(new AWSStaticCredentialsProvider(awsCred))
                    .withRegion(Regions.DEFAULT_REGION)
                    .build();
            initializeConnectionTime = new Date().getTime();
        }
        // function
        try{
            ObjectStorage.readBlob(s3Client,containerName,objectName);
        }catch  (Exception e) {
            inspector.addAttribute("duration", new Date().getTime()-initializeConnectionTime);
            e.printStackTrace();
            connect = false;
        }
        //*********************Function end
        getMetrics(isMac,inspector,count,actual_count,connect,initializeConnectionTime);
        return inspector.finish();
    }
    @Override
    public HashMap<String, Object> handleRequest(Request request, Context context) {
        return procedure(request,context);
    }
}
