/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.ipc;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.Promise;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.security.SaslStatus;
import org.apache.hadoop.hbase.security.SaslUtil;

import java.io.IOException;

/**
 * Handler for dealing with preamble response.
 */
public class PreambleResponseHandler extends ChannelDuplexHandler {
  private static final Log LOG = LogFactory.getLog(PreambleResponseHandler.class);

  private final boolean clientAllowFallback;
  private final Promise<NettyRpcConnection.Result> promise;

  public PreambleResponseHandler(Promise<NettyRpcConnection.Result> promise,
    boolean clientAllowFallback) {
    this.promise = promise;
    this.clientAllowFallback = clientAllowFallback;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    ByteBuf buf = (ByteBuf) msg;
    int status = buf.readInt();
    if (status == SaslStatus.SUCCESS.state) {
      if (buf.readInt() == SaslUtil.SWITCH_TO_SIMPLE_AUTH) {
        if (clientAllowFallback) {
          LOG.warn("Server asked us to fall back to SIMPLE auth. Falling back...");
          promise.trySuccess(NettyRpcConnection.Result.FALLBACK);
        } else {
          promise.trySuccess(NettyRpcConnection.Result.FAILURE);
          promise.tryFailure(new FallbackDisallowedException());
        }
      } else {
        promise.trySuccess(NettyRpcConnection.Result.SUCCESS);
      }
    } else {
      promise.trySuccess(NettyRpcConnection.Result.FAILURE);
      promise.tryFailure(new IOException("Error while establishing connection to server"));
    }
    buf.release();
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) {
    ctx.flush();
  }
}
