/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.security.token;

import com.google.protobuf.ByteString;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.ipc.AbstractRpcClient;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos.TokenIdentifier.Kind;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;

/**
 * Utility methods for obtaining authentication tokens, that do not require hbase-server.
 */
@InterfaceAudience.Public
public final class ClientTokenUtil {
  private static final Logger LOG = LoggerFactory.getLogger(ClientTokenUtil.class);

  // Set in TestClientTokenUtil via reflection
  private static ServiceException injectedException;

  private ClientTokenUtil() {}

  private static void injectFault() throws ServiceException {
    if (injectedException != null) {
      throw injectedException;
    }
  }

  /**
   * Obtain and return an authentication token for the current user.
   * @param conn The HBase cluster connection
   * @throws IOException if a remote error or serialization problem occurs.
   * @return the authentication token instance
   */
  @InterfaceAudience.Private
  public static Token<AuthenticationTokenIdentifier> obtainToken(
      Connection conn) throws IOException {
    // If the conn is null, we do not to check the local digest properties and
    // go with the old logic.
    if (conn != null) {
      // If we turn on the subjective digest RPC authentication,
      // just acquire the token from local conf.
      Configuration conf = conn.getConfiguration();
      if (User.isHBaseDigestAuthEnabled(conf)) {
        AuthenticationTokenIdentifier ident =
          new AuthenticationTokenIdentifier(User.getCurrent().getShortName());
        String localPassword = conf.get(User.DIGEST_PASSWORD_KEY);
        Token<AuthenticationTokenIdentifier> token = null;

        if (conn instanceof ClusterConnection) {
          token = new Token<>(ident.getBytes(), Bytes.toBytes(localPassword),
            AuthenticationTokenIdentifier.AUTH_TOKEN_TYPE,
            new Text(((ClusterConnection) conn).getClusterId()));
          return token;
        } else {
          throw new IOException("Cannot get clusterId from connection: " + conn
            + " obtain token failed.");
        }
      }
    }
    // Otherwise, we fetch token through one RPC.
    Table meta = null;
    try {
      injectFault();

      meta = conn.getTable(TableName.META_TABLE_NAME);
      CoprocessorRpcChannel rpcChannel = meta.coprocessorService(
              HConstants.EMPTY_START_ROW);
      AuthenticationProtos.AuthenticationService.BlockingInterface service =
          AuthenticationProtos.AuthenticationService.newBlockingStub(rpcChannel);
      AuthenticationProtos.GetAuthenticationTokenResponse response =
              service.getAuthenticationToken(null,
          AuthenticationProtos.GetAuthenticationTokenRequest.getDefaultInstance());

      return toToken(response.getToken());
    } catch (ServiceException se) {
      throw ProtobufUtil.handleRemoteException(se);
    } finally {
      if (meta != null) {
        meta.close();
      }
    }
  }

  /**
   * Converts a Token instance (with embedded identifier) to the protobuf representation.
   *
   * @param token the Token instance to copy
   * @return the protobuf Token message
   */
  @InterfaceAudience.Private
  static AuthenticationProtos.Token toToken(Token<AuthenticationTokenIdentifier> token) {
    AuthenticationProtos.Token.Builder builder = AuthenticationProtos.Token.newBuilder();
    builder.setIdentifier(ByteString.copyFrom(token.getIdentifier()));
    builder.setPassword(ByteString.copyFrom(token.getPassword()));
    if (token.getService() != null) {
      builder.setService(ByteString.copyFromUtf8(token.getService().toString()));
    }
    return builder.build();
  }

  /**
   * Converts a protobuf Token message back into a Token instance.
   *
   * @param proto the protobuf Token message
   * @return the Token instance
   */
  @InterfaceAudience.Private
  static Token<AuthenticationTokenIdentifier> toToken(AuthenticationProtos.Token proto) {
    return new Token<>(
        proto.hasIdentifier() ? proto.getIdentifier().toByteArray() : null,
        proto.hasPassword() ? proto.getPassword().toByteArray() : null,
        AuthenticationTokenIdentifier.AUTH_TOKEN_TYPE,
        proto.hasService() ? new Text(proto.getService().toStringUtf8()) : null);
  }

  /**
   * Obtain and return an authentication token for the given user.
   * @param conn The HBase cluster connection
   * @param user The user to obtain a token for
   * @return the authentication token instance
   */
  @InterfaceAudience.Private
  static Token<AuthenticationTokenIdentifier> obtainToken(
      final Connection conn, User user) throws IOException, InterruptedException {
    return user.runAs(new PrivilegedExceptionAction<Token<AuthenticationTokenIdentifier>>() {
      @Override
      public Token<AuthenticationTokenIdentifier> run() throws Exception {
        return obtainToken(conn);
      }
    });
  }

  /**
   * Obtain an authentication token for the given user and add it to the
   * user's credentials.
   * @param conn The HBase cluster connection
   * @param user The user for whom to obtain the token
   * @throws IOException If making a remote call to the authentication service fails
   * @throws InterruptedException If executing as the given user is interrupted
   */
  public static void obtainAndCacheToken(final Connection conn,
      User user)
      throws IOException, InterruptedException {
    try {
      Token<AuthenticationTokenIdentifier> token = obtainToken(conn, user);

      if (token == null) {
        throw new IOException("No token returned for user " + user.getName());
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Obtained token " + token.getKind().toString() + " for user " +
            user.getName());
      }
      user.addToken(token);
    } catch (IOException | InterruptedException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new UndeclaredThrowableException(e,
          "Unexpected exception obtaining token for user " + user.getName());
    }
  }

  /**
   * Set user's password, if there is no authentication token, will create one.
   */
  public static void setUserPassword(User user, String password) {
    Objects.requireNonNull(user, "User is null, when set auth info.");
    Objects.requireNonNull(password, "Null password is not allowed.");

    Token<? extends TokenIdentifier> token =
      AbstractRpcClient.TOKEN_HANDLERS.get(Kind.HBASE_AUTH_TOKEN)
        .selectToken(new Text(HConstants.CLUSTER_ID_DEFAULT), user.getTokens());

    if (token == null) {
      AuthenticationTokenIdentifier identifier =
        new AuthenticationTokenIdentifier(user.getShortName());
      Token<AuthenticationTokenIdentifier> newToken = new Token<>(identifier.getBytes(),
        Bytes.toBytes(password), identifier.getKind(), new Text(HConstants.CLUSTER_ID_DEFAULT));

      user.addToken(newToken);
      user.getUGI().setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.TOKEN);
    }
  }
}
