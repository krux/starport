package sun.net.www.protocol.ssm;

import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagement;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder;
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterRequest;
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterResult;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.nio.charset.StandardCharsets;

public class Handler extends URLStreamHandler {

    protected URLConnection openConnection(URL url) throws IOException {

        return new URLConnection(url) {

            AWSSimpleSystemsManagement ssmClient;

            @Override
            public InputStream getInputStream() throws IOException {
                String ssmPath = "/" + url.getHost() + url.getPath();
                GetParameterRequest ssmRequest = new GetParameterRequest().withName(ssmPath);
                if ("ssm+secure".equals(url.getProtocol())) {
                   ssmRequest.setWithDecryption(Boolean.TRUE);
                }
                GetParameterResult ssmResult = ssmClient.getParameter(ssmRequest);
                String ssmValue = ssmResult.getParameter().getValue();
                return new ByteArrayInputStream(ssmValue.getBytes(StandardCharsets.UTF_8));
            }

            @Override
            public void connect() throws IOException {
                ssmClient = AWSSimpleSystemsManagementClientBuilder.defaultClient();
            }

        };
    }
}
