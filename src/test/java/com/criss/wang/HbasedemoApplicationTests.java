package com.criss.wang;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.commons.io.IOUtils;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class HbasedemoApplicationTests {



	public void test() throws IOException {
		URL url = new URL("http://localhost:9991/push/pushMsg");
		HttpURLConnection urlc = (HttpURLConnection) url.openConnection();
		urlc.setDoInput(true);
		urlc.setDoOutput(true);
		urlc.setRequestMethod("POST");
		urlc.setRequestProperty("Accept", "application/x-protobuf");
		urlc.setRequestProperty("Content-Type", "application/x-protobuf");

		urlc.connect();

		try (OutputStream os = urlc.getOutputStream();) {
			System.out.println(IOUtils.copy(new ByteArrayInputStream("".getBytes()), os));
		}
		try (BufferedReader br = new BufferedReader(new InputStreamReader(urlc.getInputStream(), "utf-8"));) {
			String line = null;
			while ((line = br.readLine()) != null)
				System.out.println(line);
		}
	}
}

