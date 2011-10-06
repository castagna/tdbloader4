package org.apache.jena.tdbloader3;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;

public class TestInetAddress {

	public void test() throws UnknownHostException, SocketException {
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
