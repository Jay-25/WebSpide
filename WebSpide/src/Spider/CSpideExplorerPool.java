/**
 * @Title: CSpider.java
 * @Package Spider
 * @Description: TODO
 * @author
 * @date 2016-5-10 下午3:06:53
 * @version V1.0
 */
package Spider;

import java.util.HashMap;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.AbandonedConfig;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.logging.log4j.Logger;

import Log.CLog;

import com.gargoylesoftware.htmlunit.BrowserVersion;

/**
 * @Copyright：2016
 * @Project：WebSpide
 * @Description：
 * @Class：Spider.CSpider
 * @author：Zhao Jietong
 * @Create：2016-5-10 下午3:06:53
 * @version V1.0
 */
public class CSpideExplorerPool {

	private static Logger logger = CLog.getLogger();

	public static class PooledClientFactory {

		private BrowserVersion                    browserVersion = null;
		private GenericObjectPool<CSpideExplorer> clientPool     = null;

		public PooledClientFactory(BrowserVersion browserVersion) {
			this.browserVersion = browserVersion;
			init(browserVersion);
		}

		@Override
		protected void finalize() throws Throwable {
			clientPool.clear();
			clientPool = null;
			super.finalize();
		}

		private void init(BrowserVersion browserVersion) {
			logger.info("Init SpideExplorerPool");
			final PooledClientFactory _this = this;
			GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
			// 设置的逐出策略类名, 默认DefaultEvictionPolicy(当连接超过最大空闲时间,或连接数超过最大空闲连接数)
			poolConfig.setEvictionPolicyClassName("org.apache.commons.pool2.impl.DefaultEvictionPolicy");
			// 是否启用后进先出, 默认true
			poolConfig.setLifo(true);
			// 最大连接数
			poolConfig.setMaxTotal(500);
			// 最大空闲连接数
			poolConfig.setMaxIdle(100);
			// 最小空闲连接数, 默认0
			poolConfig.setMinIdle(0);
			// 获取连接时的最大等待毫秒数(如果设置为阻塞时BlockWhenExhausted),如果超时就抛异常, 小于零:阻塞不确定的时间, 默认-1
			poolConfig.setMaxWaitMillis(60 * 1000);
			// 对象空闲多久后逐出, 当空闲时间>该值 且 空闲连接>最大空闲数 时直接逐出,不再根据MinEvictableIdleTimeMillis判断 (默认逐出策略)
			poolConfig.setMinEvictableIdleTimeMillis(30 * 1000);
			// 逐出扫描的时间间隔(毫秒) 如果为负数,则不运行逐出线程, 默认-1
			poolConfig.setTimeBetweenEvictionRunsMillis(60 * 1000);
			// 连接耗尽时是否阻塞, false报异常,ture阻塞直到超时, 默认true
			poolConfig.setBlockWhenExhausted(true);
			// 在空闲时检查有效性, 默认false
			poolConfig.setTestWhileIdle(false);
			// 出借测试
			poolConfig.setTestOnBorrow(false);
			// 归还测试
			poolConfig.setTestOnReturn(false);
			//
			AbandonedConfig abandonedConfig = new AbandonedConfig();
			abandonedConfig.setLogAbandoned(false);
			abandonedConfig.setRemoveAbandonedOnBorrow(true);
			abandonedConfig.setRemoveAbandonedOnMaintenance(true);
			abandonedConfig.setRemoveAbandonedTimeout(60 * 1000);
			//
			clientPool = new GenericObjectPool<CSpideExplorer>(new PooledObjectFactory<CSpideExplorer>() {

				@Override
				public void activateObject(PooledObject<CSpideExplorer> arg0) throws Exception {
				}

				@Override
				public void destroyObject(PooledObject<CSpideExplorer> arg0) throws Exception {
					CSpideExplorer client = arg0.getObject();
					client.close();
					client = null;
				}

				@Override
				public PooledObject<CSpideExplorer> makeObject() throws Exception {
					final CSpideExplorer client = new CSpideExplorer();
					client.setOwnerPooledClientFactory(_this);
					return new DefaultPooledObject<CSpideExplorer>(client);
				}

				@Override
				public void passivateObject(PooledObject<CSpideExplorer> arg0) throws Exception {
				}

				@Override
				public boolean validateObject(PooledObject<CSpideExplorer> arg0) {
					return false;
				}
			}, poolConfig, abandonedConfig);
		}

		public CSpideExplorer getSpideExplorer() {
			int retry = 5;
			while (retry-- > 0) {
				try {
					return clientPool.borrowObject();
				}
				catch (Exception e) {
					logger.error("borrow explorer again [" + e.getMessage() + "]", e);
					CSpideExplorer.sleep(2000);
					clientPool.close();
					clientPool.clear();
					init(browserVersion);
				}
			}
			return null;
		}

		protected void returnSpideExplorer(CSpideExplorer client) {
			try {
				clientPool.returnObject(client);
			}
			catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}
	}

	private static HashMap<BrowserVersion, PooledClientFactory> instances = new HashMap<BrowserVersion, PooledClientFactory>();

	private CSpideExplorerPool(BrowserVersion explorer) {
	}

	@Override
	protected void finalize() throws Throwable {
		instances.clear();
		instances = null;
		super.finalize();
	}

	public static PooledClientFactory getInstance(BrowserVersion browserVersion) {
		if (instances.containsKey(browserVersion)) {
			return instances.get(browserVersion);
		}
		else {
			PooledClientFactory instance = new PooledClientFactory(browserVersion);
			instances.put(browserVersion, instance);
			return instance;
		}
	}
}
