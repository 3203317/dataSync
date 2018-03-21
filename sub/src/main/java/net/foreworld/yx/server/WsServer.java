package net.foreworld.yx.server;

import net.foreworld.util.Server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 *
 * @author huangxin <3203317@qq.com>
 *
 */
@Component
public class WsServer extends Server {

	private static final Logger logger = LoggerFactory
			.getLogger(WsServer.class);

	@Override
	public void start() {

		try {
			logger.info("start");
		} catch (Exception e) {
			logger.error("", e);
			System.exit(1);
		} finally {
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					shutdown();
				}
			});
		}
	}

	@Override
	public void shutdown() {
		logger.info("shutdown");
	}

}
