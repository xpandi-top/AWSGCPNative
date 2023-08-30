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
import java.util.*;

import static basic.FileTransform.transform;
import static basic.MetadataRetriever.*;


/**
 * uwt.lambda_test::handleRequest
 *
 * @author Wes Lloyd
 * @author Robert Cordingly
 * @author dimo
 */
public class Transform implements RequestHandler<Request, HashMap<String, Object>> {
    private static Long initializeConnectionTime ;
    static BasicAWSCredentials awsCred = new BasicAWSCredentials(identity, credential);
    static AmazonS3 s3Client;
    public HashMap<String, Object> handleRequest(Request request, Context context) {
        Inspector inspector = new Inspector();
        if (!isMac){
            inspector.inspectAll();
        }
        //***********Function start
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
        //get object file using source bucket and srcKey name
        try {
            InputStream inputStream = ObjectStorage.readBlobContent(s3Client,containerName,objectName);
            String content = transform(inputStream);
            ObjectStorage.writeBlob(s3Client,containerName,"Transform_"+objectName,content);
        } catch (Exception e) {
            e.printStackTrace();
            connect = false;
            inspector.addAttribute("duration", new Date().getTime()-initializeConnectionTime);
        }
        //****************END FUNCTION IMPLEMENTATION***************************
        //Collect final information such as total runtime and cpu deltas.
        getMetrics(isMac, inspector, count, actual_count, connect, initializeConnectionTime);
        return inspector.finish();
    }
}