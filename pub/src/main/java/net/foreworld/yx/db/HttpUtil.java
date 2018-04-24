package net.foreworld.yx.db;

import net.foreworld.yx.db.model.SendData;
import okhttp3.FormBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 *
 * @author huangxin
 *
 */
@EnableScheduling
@Component
public class HttpUtil {

	private static String URL;

	@Value("${publish.url}")
	private void setUrl(String url) {
		URL = url;
	}

	private static String TOPIC;

	@Value("${publish.topic}")
	private void setTopic(String topic) {
		TOPIC = topic;
	}

	private static final Logger logger = LoggerFactory
			.getLogger(HttpUtil.class);

	@Scheduled(cron = "${publish.interval}")
	public void sc() {
		SendData sql = MonitorDbUtil.getSql(null);

		if (null != sql)
			rpc(sql);
	}

	/**
	 *
	 * @param sd
	 */
	private static void rpc(SendData sd) {
		logger.info("{}:{}", sd.getPos(), sd.getSql());

		OkHttpClient client = new OkHttpClient();

		try {
			FormBody.Builder formBody = new FormBody.Builder();
			// 1.0.1 -->
			// formBody.add("topicId", TOPIC);
			// <-- 1.0.1
			formBody.add("detail", sd.getSql());

			Request.Builder request = new Request.Builder().url(URL)
					.post(formBody.build())
					.addHeader("Content-Type", "application/json");

			Response response = client.newCall(request.build()).execute();
			if (response.isSuccessful()) {
				logger.info(response.body().string());
				MonitorDbUtil.setCfgPos(sd.getPos());
				return;
			}

			logger.error("{}:{}", sd.getPos(), sd.getSql());

		} catch (Exception e) {
			logger.error("{}:{}", sd.getPos(), sd.getSql());
		}
	}
}
