package functions;

import basic.Request;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.functions.HttpFunction;
import com.google.cloud.functions.HttpRequest;
import com.google.cloud.functions.HttpResponse;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import saaf.Inspector;

import java.io.FileInputStream;
import java.util.Date;

import static basic.MetadataRetriever.*;
import static basic.ObjectStorage.deleteBlob;

public class DeleteMultiple implements HttpFunction {
    private static long initializeConnectionTime;
    private static Storage storage;

    @Override
    public void service(HttpRequest httpRequest, HttpResponse httpResponse) throws Exception {
        Inspector inspector = new Inspector();
        if (!isMac){
            inspector.inspectAll();
        }
        //***********Function start
        // get parameters
        Request request = gson.fromJson(httpRequest.getReader(),Request.class);
        int count = 2;
        int actual_count = 0;
        boolean connect = true;
        if (request!=null&&request.getObjectName()!=null) objectName = request.getObjectName();
        if (request!=null && request.getCount()>0) count = request.getCount();
        if (request != null && request.getContainerName() != null) containerName = request.getContainerName();
        // initialize storage
        if (storage==null){
            storage = StorageOptions
                    .newBuilder()
                    .setProjectId(projectId)
                    .setCredentials(GoogleCredentials.fromStream(new FileInputStream(credential)))
                    .build().getService();
            initializeConnectionTime = new Date().getTime();
            System.out.println("start initializing");
        }

        for (int i=0; i<count;i++){
            try {
                deleteBlob(storage,containerName,objectName+i);
            } catch (Exception e) {
                e.printStackTrace();
                actual_count = i;
                connect = false;
                inspector.addAttribute("duration", new Date().getTime()-initializeConnectionTime);
                break;
            }
        }
        // collect metrics
        getResponse(httpResponse, isMac, inspector, count, actual_count, connect, initializeConnectionTime);

    }
}
