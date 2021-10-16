/**
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
package org.apache.hadoop.hbase.thrift.ldap;

import java.util.Properties;
import javax.naming.Context;
import javax.naming.directory.InitialDirContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Utilities class for LDAP authentication.
 */
@InterfaceAudience.Private
public class LdapUtilities {
  private static final Log LOG = LogFactory.getLog(LdapUtilities.class);

  public static boolean authenticate(String username, String password,
      String ldapUrl, String dcFormat) {
    Properties props = new Properties();
    String userDc = String.format(dcFormat, username);

    props.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
    props.put(Context.PROVIDER_URL, ldapUrl);
    props.put(Context.SECURITY_PRINCIPAL, userDc);
    props.put(Context.SECURITY_CREDENTIALS, password);
    try {
      InitialDirContext context = new InitialDirContext(props);
      context.close();
    } catch (Exception e) {
      LOG.warn("Ldap authentication failed.", e);
      return false;
    }
    return true;
  }
}
