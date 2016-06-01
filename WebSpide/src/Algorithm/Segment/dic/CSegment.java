/**
 * @Title: CSegment.java
 * @Package Algorithm.Segment.dic
 * @Description: TODO
 * @author
 * @date 2014-9-25 下午4:34:21
 * @version V1.0
 */
package Algorithm.Segment.dic;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.ansj.library.UserDefineLibrary;
import org.ansj.splitWord.analysis.ToAnalysis;

import System.Directory.CFileWatcher;

/**
 * @Copyright：2014
 * @Project：Spide
 * @Description：https://github.com/NLPchina/ansj_seg
 * @Description：http://nlpchina.github.io/ansj_seg/
 * @Class：Algorithm.Segment.dic.CSegment
 * @author：Zhao Jietong
 * @Create：2014-9-25 下午4:34:21
 * @version V1.0
 */
/*
 * # 1. 名词 (1个一类，7个二类，5个三类)
 * 名词分为以下子类：
 * n 名词
 * nr 人名
 * nr1 汉语姓氏
 * nr2 汉语名字
 * nrj 日语人名
 * nrf 音译人名
 * ns 地名
 * nsf 音译地名
 * nt 机构团体名
 * nz 其它专名
 * nl 名词性惯用语
 * ng 名词性语素
 * nw 新词
 * # 2. 时间词(1个一类，1个二类)
 * t 时间词
 * tg 时间词性语素
 * # 3. 处所词(1个一类)
 * s 处所词
 * # 4. 方位词(1个一类)
 * f 方位词
 * # 5. 动词(1个一类，9个二类)
 * v 动词
 * vd 副动词
 * vn 名动词
 * vshi 动词“是”
 * vyou 动词“有”
 * vf 趋向动词
 * vx 形式动词
 * vi 不及物动词（内动词）
 * vl 动词性惯用语
 * vg 动词性语素
 * # 6. 形容词(1个一类，4个二类)
 * a 形容词
 * ad 副形词
 * an 名形词
 * ag 形容词性语素
 * al 形容词性惯用语
 * # 7. 区别词(1个一类，2个二类)
 * b 区别词
 * bl 区别词性惯用语
 * # 8. 状态词(1个一类)
 * z 状态词
 * # 9. 代词(1个一类，4个二类，6个三类)
 * r 代词
 * rr 人称代词
 * rz 指示代词
 * rzt 时间指示代词
 * rzs 处所指示代词
 * rzv 谓词性指示代词
 * ry 疑问代词
 * ryt 时间疑问代词
 * rys 处所疑问代词
 * ryv 谓词性疑问代词
 * rg 代词性语素
 * # 10. 数词(1个一类，1个二类)
 * m 数词
 * mq 数量词
 * # 11. 量词(1个一类，2个二类)
 * q 量词
 * qv 动量词
 * qt 时量词
 * # 12. 副词(1个一类)
 * d 副词
 * # 13. 介词(1个一类，2个二类)
 * p 介词
 * pba 介词“把”
 * pbei 介词“被”
 * # 14. 连词(1个一类，1个二类)
 * c 连词
 * cc 并列连词
 * # 15. 助词(1个一类，15个二类)
 * u 助词
 * uzhe 着
 * ule 了 喽
 * uguo 过
 * ude1 的 底
 * ude2 地
 * ude3 得
 * usuo 所
 * udeng 等 等等 云云
 * uyy 一样 一般 似的 般
 * udh 的话
 * uls 来讲 来说 而言 说来
 * uzhi 之
 * ulian 连 （“连小学生都会”）
 * # 16. 叹词(1个一类)
 * e 叹词
 * # 17. 语气词(1个一类)
 * y 语气词(delete yg)
 * # 18. 拟声词(1个一类)
 * o 拟声词
 * # 19. 前缀(1个一类)
 * h 前缀
 * # 20. 后缀(1个一类)
 * k 后缀
 * # 21. 字符串(1个一类，2个二类)
 * x 字符串
 * xx 非语素字
 * xu 网址URL
 * # 22. 标点符号(1个一类，16个二类)
 * w 标点符号
 * wkz 左括号，全角：（ 〔 ［ ｛ 《 【 〖〈 半角：( [ { <
 * wky 右括号，全角：） 〕 ］ ｝ 》 】 〗 〉 半角： ) ] { >
 * wyz 左引号，全角：“ ‘ 『
 * wyy 右引号，全角：” ’ 』
 * wj 句号，全角：。
 * ww 问号，全角：？ 半角：?
 * wt 叹号，全角：！ 半角：!
 * wd 逗号，全角：， 半角：,
 * wf 分号，全角：； 半角： ;
 * wn 顿号，全角：、
 * wm 冒号，全角：： 半角： :
 * ws 省略号，全角：…… …
 * wp 破折号，全角：—— －－ ——－ 半角：--- ----
 * wb 百分号千分号，全角：％ ‰ 半角：%
 * wh 单位符号，全角：￥ ＄ ￡ ° ℃ 半角：$
 */

public class CSegment extends ToAnalysis {

	private class _CSogouDicCustomCallback implements IDicCustomCallback {

		@Override
		public synchronized void callback(Object object) {
			CDicSogou.WordLibrary word = (CDicSogou.WordLibrary) object;
			UserDefineLibrary.insertWord(word.getWord(), word.getNature(), 1000);
		}
	}

	private String                         dicPath                = "";
	private static CSegment                segment                = null;
	private final CDicSogou                dicSogou               = new CDicSogou();
	private final _CSogouDicCustomCallback sogouDicCustomCallback = new _CSogouDicCustomCallback();

	private CSegment() {
		new CFileWatcher("library/custom", ".dic", new CFileWatcher.IFileWatcherDeal() {

			@Override
			public void deal(String fileName, String event) {
				if (event.equals("ENTRY_CREATE") || event.equals("ENTRY_MODIFY")) {
					System.out.println("Load " + fileName);
					customLoadDic(fileName);
				}
				else if (event.equals("ENTRY_DELETE")) {
				}
			}
		});
		//
		new CFileWatcher("library/sogou", ".scel", new CFileWatcher.IFileWatcherDeal() {

			@Override
			public void deal(String fileName, String event) {
				if (event.equals("ENTRY_CREATE") || event.equals("ENTRY_MODIFY")) {
					try {
						System.out.println("Load " + fileName);
						dicSogou.load(fileName, sogouDicCustomCallback);
					}
					catch (IOException e) {
					}
				}
				else if (event.equals("ENTRY_DELETE")) {
				}
			}
		});
	}

	public static CSegment getInstance() {
		if (segment == null) {
			segment = new CSegment();
		}
		return segment;
	}

	public void loadCustomDic() {
		loadCustomDic(".");
	}

	public void loadCustomDic(String dicPath) {
		this.dicPath = dicPath;
		loadCustomDicFromCustom();
		loadCustomDicFromSoGou();
		loadCustomDicFromVocabulary();
	}

	private void customLoadDic(String fileName) {
		File file = new File(fileName);
		if (file.exists()) {
			InputStream fis = null;
			BufferedReader br = null;
			try {
				String word = null;
				fis = new FileInputStream(file);
				br = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
				for (int timeout = 300; timeout > 0 && !br.ready(); timeout--) {
					Thread.sleep(10);
				}
				while ((word = br.readLine()) != null) {
					if (word.length() == 0) continue;
					String[] ws = word.trim().split("	");
					if (ws.length > 2) {
						ws[0] = ws[0].trim();
						ws[1] = ws[1].trim();
						ws[2] = ws[2].trim();
						int wfq = 1000;
						try {
							wfq = Integer.parseInt(ws[1]);
						}
						catch (Exception e) {
						}
						if (ws[2].length() == 0) ws[2] = "userDefine";
						UserDefineLibrary.insertWord(ws[0], ws[2], wfq);
					}
					else if (ws.length > 0) {
						UserDefineLibrary.insertWord(ws[0], "userDefine", 1000);
					}
				}
			}
			catch (Exception e) {
			}
		}
		file = null;
	}

	public void loadCustomDicFromVocabulary() {
		File dir = new File(dicPath + File.separator + "library/vocabulary");
		if (!dir.exists()) {
			dir.mkdirs();
			return;
		}
		//
		CVocabulary vocabulary = new CVocabulary();
		vocabulary.loadDic(dir.listFiles(new FilenameFilter() {

			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".dic");
			}
		}));
		//
		for (String word : vocabulary.getAllWords()) {
			UserDefineLibrary.insertWord(word, "userDefine", 1000);
		}
	}

	public void loadCustomDicFromCustom() {
		File dir = new File(dicPath + File.separator + "library/custom");
		if (!dir.exists()) {
			dir.mkdirs();
			return;
		}
		File[] filesOrDirs = dir.listFiles(new FilenameFilter() {

			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".dic");
			}
		});
		ExecutorService pool = Executors.newFixedThreadPool(8);
		for (int i = 0; i < filesOrDirs.length; i++) {
			if (!filesOrDirs[i].isDirectory()) {
				final File file = filesOrDirs[i];
				pool.execute(new Runnable() {

					@Override
					public void run() {
						customLoadDic(file.getAbsolutePath());
						System.out.println("Load " + file.getAbsolutePath());
					}
				});
			}
		}
		pool.shutdown();
		try {
			pool.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
		}
		catch (InterruptedException e) {
		}
		pool = null;
		dir = null;
	}

	public void loadCustomDicFromSoGou() {
		File dir = new File(dicPath + File.separator + "library/sogou");
		if (!dir.exists()) {
			dir.mkdirs();
			return;
		}
		File[] filesOrDirs = dir.listFiles(new FilenameFilter() {

			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".scel");
			}
		});
		ExecutorService pool = Executors.newFixedThreadPool(8);
		for (int i = 0; i < filesOrDirs.length; i++) {
			if (!filesOrDirs[i].isDirectory()) {
				final File file = filesOrDirs[i];
				pool.execute(new Runnable() {

					@Override
					public void run() {
						try {
							dicSogou.load(file.getAbsolutePath(), sogouDicCustomCallback);
							System.out.println("Load " + file.getAbsolutePath());
						}
						catch (IOException e) {
						}
					}
				});
			}
		}
		pool.shutdown();
		try {
			pool.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
		}
		catch (InterruptedException e) {
		}
		pool = null;
		dir = null;
	}
}
