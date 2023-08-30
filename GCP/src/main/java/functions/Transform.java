
package functions;


import basic.ObjectStorage;
import basic.Request;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.functions.HttpFunction;
import com.google.cloud.functions.HttpRequest;
import com.google.cloud.functions.HttpResponse;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import saaf.Inspector;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Date;

import static basic.FileTransform.transform;
import static basic.MetadataRetriever.*;

public class Transform implements HttpFunction {
    static Storage storage;
    private static Long initializeConnectionTime;
//    private static final Logger logger = Logger.getLogger(Transform.class.getName());
    @Override
    public void service(HttpRequest httpRequest, HttpResponse response)
            throws IOException {
        //collect initial data
        Inspector inspector = new Inspector();
        if (!isMac){
            inspector.inspectAll();
        }
        // get parameters
        Request request = gson.fromJson(httpRequest.getReader(),Request.class);
        int count = 2;
        int actual_count = 0;
        boolean connect = true;
        if (request!=null&&request.getObjectName()!=null) objectName = request.getObjectName();
        if (request!=null && request.getCount()>0) count = request.getCount();
        if (request != null && request.getContainerName() != null) containerName = request.getContainerName();
        // build storage
        if (storage==null){
            storage = StorageOptions
                    .newBuilder()
                    .setProjectId(projectId)
                    .setCredentials(GoogleCredentials.fromStream(new FileInputStream(credential)))
                    .build().getService();
            initializeConnectionTime = new Date().getTime();
            System.out.println("start initializing");
        }
        // function
        try{
            ReadChannel reader = ObjectStorage.readBlobContent(storage,containerName,objectName);
            String content = transform(reader);
            ObjectStorage.writeBlob(storage,containerName,"Transform_"+objectName,content);
        }catch (Exception e){
            e.printStackTrace();
            connect = false;
            inspector.addAttribute("duration", new Date().getTime()-initializeConnectionTime);
        }
        // collects
        getResponse(response, isMac, inspector, count, actual_count, connect, initializeConnectionTime);
    }

}