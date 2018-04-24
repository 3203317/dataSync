package net.foreworld.yx;

import javax.annotation.Resource;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import net.foreworld.yx.server.WsServer;

/**
 *
 * @author huangxin <3203317@qq.com>
 *
 */
@SpringBootApplication
@ComponentScan("net.foreworld.yx")
public class RunWsServer implements CommandLineRunner {

	@Resource(name = "wsServer")
	private WsServer wsServer;

	public static void main(String[] args) {
		SpringApplication.run(RunWsServer.class, args);
	}

	public void run(String... strings) throws InterruptedException {
		wsServer.start();
		Thread.currentThread().join();
	}

}
