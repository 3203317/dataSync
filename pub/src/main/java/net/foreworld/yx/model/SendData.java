package net.foreworld.yx.model;

/**
 *
 * @author huangxin
 *
 */
public class SendData {

	private Long pos;
	private String sql;

	private String topic_name;

	public String getTopic_name() {
		return topic_name;
	}

	public void setTopic_name(String topic_name) {
		this.topic_name = topic_name;
	}

	public Long getPos() {
		return pos;
	}

	public void setPos(Long pos) {
		this.pos = pos;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

}
