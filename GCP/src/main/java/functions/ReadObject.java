package functions;

import basic.ObjectStorage;
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

public class ReadObject implements HttpFunction {
    private static long initializeConnectionTime;
    private static Storage storage;

    @Override
    public void service(HttpRequest httpRequest, HttpResponse httpResponse) throws Exception {
        Inspector inspector = new Inspector();
        if (!isMac) {
            inspector.inspectAll();
        }
        //*************FunctionStart**************
        // get parameters
        Request request = gson.fromJson(httpRequest.getReader(), Request.class);
        int count = 2;
        boolean connect = true;
        int actual_count = 0;
        if (request != null && request.getObjectName() != null) objectName = request.getObjectName();
        if (request != null && request.getContainerName() != null) containerName = request.getContainerName();
        if (request != null && request.getCount() > 0) count = request.getCount();
        // initialize storage
        if (storage == null) {
            storage = StorageOptions
                    .newBuilder()
                    .setProjectId(projectId)
                    .setCredentials(GoogleCredentials.fromStream(new FileInputStream(credential)))
                    .build().getService();
            initializeConnectionTime = new Date().getTime();
            System.out.println("start initializing");
        }
        // function
        try {
            ObjectStorage.readBlob(storage, containerName, objectName);
        } catch (Exception e) {
            e.printStackTrace();
            connect = false;
            inspector.addAttribute("duration", new Date().getTime() - initializeConnectionTime);
        }
        //*********************Function end
        getResponse(httpResponse, isMac, inspector, count, actual_count, connect, initializeConnectionTime);
    }
}
