package net.foreworld.yx.server;

import javax.annotation.Resource;
import javax.jms.JMSException;
import javax.jms.Topic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jms.core.JmsMessagingTemplate;
import org.springframework.stereotype.Component;

import com.google.code.or.OpenReplicator;

import net.foreworld.util.Server;
import net.foreworld.yx.db.DbUtil;
import net.foreworld.yx.db.MonitorDbUtil;
import net.foreworld.yx.listener.MyBinlogEventListener;
import net.foreworld.yx.model.BinlogMasterStatus;

/**
 *
 * @author huangxin <3203317@qq.com>
 *
 */
@PropertySource("classpath:activemq.properties")
@Component
public class WsServer extends Server {

	@Value("${queue.topic.send}.${topic.id}")
	private String topic_name;

	@Resource(name = "myBinlogEventListener")
	private MyBinlogEventListener myBinlogEventListener;

	Topic t = new Topic() {
		@Override
		public String getTopicName() throws JMSException {
			return topic_name;
		}
	};

	@Value("${jdbc.url}")
	private String jdbc_url;

	@Value("${jdbc.port}")
	private Integer jdbc_port;

	@Value("${jdbc.user}")
	private String jdbc_user;

	@Value("${jdbc.password}")
	private String jdbc_password;

	@Value("${jdbc.server_id}")
	private Integer jdbc_server_id;

	@Value("${jdbc.binlog_position}")
	private Integer jdbc_binlog_position;

	@Value("${jdbc.binlog_file_name}")
	private String jdbc_binlog_file_name;

	@Resource(name = "jmsMessagingTemplate")
	private JmsMessagingTemplate jmsMessagingTemplate;

	private static final Logger logger = LoggerFactory.getLogger(WsServer.class);

	@Override
	public void start() {

		try {
			// jmsMessagingTemplate.convertAndSend(t, "test12345");
			mysql();
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

	private void mysql() throws Exception {
		DbUtil.reloaddbTableColumnMapMap();

		BinlogMasterStatus bms = MonitorDbUtil.getBinlogMasterStatus();

		final OpenReplicator or = new OpenReplicator();
		or.setUser(jdbc_user);
		or.setPassword(jdbc_password);
		or.setHost(jdbc_url);
		or.setPort(jdbc_port);
		or.setServerId(jdbc_server_id);
		or.setBinlogPosition(bms.getPosition());
		or.setBinlogFileName(bms.getFile_name());
		or.setBinlogEventListener(myBinlogEventListener);
		or.start();
	}
}
