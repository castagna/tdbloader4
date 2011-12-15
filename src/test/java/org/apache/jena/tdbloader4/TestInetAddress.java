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

package org.apache.jena.tdbloader4;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;

import org.junit.Test;

public class TestInetAddress {

	@Test public void test() throws UnknownHostException, SocketException {
		System.out.println(String.format("LocalHost CanonicalHostName :\t%s", InetAddress.getLocalHost().getCanonicalHostName()));
		System.out.println(String.format("LocalHost HostName :\t%s", InetAddress.getLocalHost().getHostName()));
		System.out.println();

		Enumeration<NetworkInterface> enet = NetworkInterface.getNetworkInterfaces();

		while (enet.hasMoreElements()) {
			NetworkInterface net = enet.nextElement();

			System.out.println("NetworkInterface");
			System.out.println("=================================");
			System.out.println(String.format("Name:\t%s", net.getName()));
			System.out.println(String.format("DisplayName:\t%s", net.getDisplayName()));

			Enumeration<InetAddress> eaddr = net.getInetAddresses();
			while (eaddr.hasMoreElements()) {
				InetAddress inet = eaddr.nextElement();
				System.out.println("\tInetAddress : ");
				System.out.println(String.format("\t\tCanonicalHostName:\t%s", inet.getCanonicalHostName()));
				System.out.println(String.format("\t\tHostAddress:\t\t%s", inet.getHostAddress()));
				System.out.println(String.format("\t\tHostName:\t\t%s", inet.getHostName()));
			}
			System.out.println();
		}
	}

}
