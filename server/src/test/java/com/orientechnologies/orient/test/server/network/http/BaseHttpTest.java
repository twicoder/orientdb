package com.orientechnologies.orient.test.server.network.http;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.server.OServer;
import java.io.File;
import java.io.IOException;
import org.apache.http.Consts;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

/**
 * Base test class for HTTP protocol.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com) (l.garulli--at-orientdb.com)
 */
public abstract class BaseHttpTest {
  protected String serverDirectory;

  private static OServer server;

  private String serverCfg =
      "/com/orientechnologies/orient/server/network/orientdb-server-config-httponly.xml";
  private String protocol = "http";
  private String host = "localhost";
  private int port = 2499;
  private String realm = "OrientDB-";
  private String userName = "admin";
  private String userPassword = "admin";
  private String databaseName;
  private Boolean keepAlive = null;

  private HttpRequestBase request;
  private AbstractHttpEntity payload;
  private HttpResponse response;
  private int retry = 1;

  public enum CONTENT {
    TEXT,
    JSON
  }

  public BaseHttpTest payload(final String content, final CONTENT contentType) {
    payload =
        new StringEntity(
            content,
            ContentType.create(
                contentType == CONTENT.JSON ? "application/json" : "plain/text", Consts.UTF_8));
    return this;
  }

  protected void startServer() throws Exception {
    if (server == null) {
      server = new OServer(false);
      if (serverDirectory != null) {
        server.setServerRootDirectory(serverDirectory);
      }
      server.startup(getClass().getResourceAsStream(getServerCfg()));
      server.activate();
    }
  }

  protected void stopServer() throws Exception {
    if (server != null) {
      server.shutdown();
      server = null;

      Orient.instance().shutdown();
      if (serverDirectory != null) {
        OFileUtils.deleteRecursively(new File(serverDirectory));
      }
      Orient.instance().startup();
    }
  }

  protected boolean isInDevelopmentMode() {
    final String env = System.getProperty("orientdb.test.env");
    return env == null || env.equals("dev");
  }

  protected BaseHttpTest exec() throws IOException {
    final HttpHost targetHost = new HttpHost(getHost(), getPort(), getProtocol());

    CredentialsProvider credsProvider = new BasicCredentialsProvider();
    credsProvider.setCredentials(
        new AuthScope(targetHost),
        new UsernamePasswordCredentials(getUserName(), getUserPassword()));

    // Create AuthCache instance
    AuthCache authCache = new BasicAuthCache();
    // Generate BASIC scheme object and add it to the local auth cache
    BasicScheme basicAuth = new BasicScheme();
    authCache.put(targetHost, basicAuth);

    // Add AuthCache to the execution context
    HttpClientContext context = HttpClientContext.create();
    context.setCredentialsProvider(credsProvider);
    context.setAuthCache(authCache);

    if (keepAlive != null) request.addHeader("Connection", keepAlive ? "Keep-Alive" : "Close");

    if (payload != null && request instanceof HttpEntityEnclosingRequestBase)
      ((HttpEntityEnclosingRequestBase) request).setEntity(payload);

    final CloseableHttpClient httpClient = HttpClients.createDefault();

    // DefaultHttpMethodRetryHandler retryhandler = new DefaultHttpMethodRetryHandler(retry, false);
    // context.setAttribute(HttpMethodParams.RETRY_HANDLER, retryhandler);

    response = httpClient.execute(targetHost, request, context);

    return this;
  }

  protected BaseHttpTest get(final String url, Header[] headers) throws IOException {
    request = new HttpGet(getBaseURL() + "/" + url);
    request.setHeaders(headers);

    response = null;
    return this;
  }

  protected BaseHttpTest get(final String url) throws IOException {
    request = new HttpGet(getBaseURL() + "/" + url);
    response = null;
    return this;
  }

  protected BaseHttpTest post(final String url) throws IOException {
    request = new HttpPost(getBaseURL() + "/" + url);
    response = null;
    return this;
  }

  protected BaseHttpTest put(final String url) throws IOException {
    request = new HttpPut(getBaseURL() + "/" + url);
    response = null;
    return this;
  }

  protected BaseHttpTest delete(final String url) throws IOException {
    request = new HttpDelete(getBaseURL() + "/" + url);
    response = null;
    return this;
  }

  protected BaseHttpTest patch(final String url) throws IOException {
    request = new HttpPatch(getBaseURL() + "/" + url);
    response = null;
    return this;
  }

  protected HttpResponse getResponse() throws IOException {
    if (response == null) exec();
    return response;
  }

  protected BaseHttpTest setKeepAlive(final boolean iValue) {
    keepAlive = iValue;
    return this;
  }

  protected String getBaseURL() {
    return getProtocol() + "://" + getHost() + ":" + getPort();
  }

  public String getUserName() {
    return userName;
  }

  protected BaseHttpTest setUserName(final String userName) {
    this.userName = userName;
    return this;
  }

  public BaseHttpTest setRetry(final int iRetry) {
    retry = iRetry;
    return this;
  }

  protected String getUserPassword() {
    return userPassword;
  }

  protected BaseHttpTest setUserPassword(final String userPassword) {
    this.userPassword = userPassword;
    return this;
  }

  protected String getProtocol() {
    return protocol;
  }

  protected String getHost() {
    return host;
  }

  protected int getPort() {
    return port;
  }

  protected String getServerCfg() {
    return serverCfg;
  }

  protected BaseHttpTest setServerCfg(String serverCfg) {
    this.serverCfg = serverCfg;
    return this;
  }

  protected String getDatabaseName() {
    return databaseName;
  }

  protected String getRealm() {
    return realm;
  }

  protected BaseHttpTest setRealm(String realm) {
    this.realm = realm;
    return this;
  }

  public static OServer getServer() {
    return server;
  }
}
