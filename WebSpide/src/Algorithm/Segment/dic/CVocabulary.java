package Algorithm.Segment.dic;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

/**
 * @Title: ccc.java
 * @Package
 * @Description: TODO
 * @author
 * @date 2014-11-11 下午2:35:40
 * @version V1.0
 */

/**
 * @Copyright：2014
 * @Project：Spide
 * @Description：
 * @Class：.ccc
 * @author：Zhao Jietong
 * @Create：2014-11-11 下午2:35:40
 * @version V1.0
 */

public class CVocabulary {

	@SuppressWarnings("serial")
	public static class WordList extends ArrayList<String> {

		private boolean wrdy = false;

		@Override
		public String toString() {
			if (size() == 0) return "";
			String result = get(0);
			for (int i = 1; i < size(); i++) {
				result += "," + get(i);
			}
			return result;
		}
	}

	@SuppressWarnings("serial")
	public static class Vocabularies extends HashMap<String, WordList> {

		private void update(String[] words) {
			WordList wlst = null;
			for (int i = 0; i < words.length; i++) {
				wlst = get(words[i]);
				if (wlst != null) break;
			}
			if (wlst == null) {
				wlst = new WordList();
			}
			for (int i = 0; i < words.length; i++) {
				put(words[i], wlst);
				if (wlst.indexOf(words[i]) == -1) wlst.add(words[i]);
			}
		}
	}

	private Vocabularies vocabularies = null;

	public CVocabulary() {
		vocabularies = new Vocabularies();
	}

	public WordList getWordList(String word) {
		return vocabularies.get(word);
	}

	public Set<String> getAllWords() {
		return vocabularies.keySet();
	}

	public void loadDic(String dicFileName) {
		loadDic(new File(dicFileName));
	}

	public void loadDic(File[] dicFileNames) {
		for (int i = 0; i < dicFileNames.length; i++) {
			loadDic(dicFileNames[i]);
		}
	}

	public void loadDic(File dicFile) {
		try {
			if (dicFile.isFile()) {
				BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(dicFile), "UTF-8"));
				for (int timeout = 100; timeout > 0 && !br.ready(); timeout--) {
					Thread.sleep(10);
				}
				String line;
				while ((line = br.readLine()) != null) {
					vocabularies.update(line.split(","));
				}
				br.close();
				System.out.println("Load " + dicFile.getAbsolutePath());
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("rawtypes")
	public void saveDic(String dicFileName) {
		OutputStreamWriter fw = null;
		try {
			fw = new OutputStreamWriter(new FileOutputStream(dicFileName), "UTF-8");
			Iterator iter = vocabularies.entrySet().iterator();
			while (iter.hasNext()) {
				Entry entry = (Entry) iter.next();
				WordList wordList = (WordList) entry.getValue();
				if (!wordList.wrdy) {
					wordList.wrdy = true;
					fw.write(wordList.toString() + "\n");
				}
			}
			fw.flush();
			fw.close();
		}
		catch (Exception e) {
		}
		fw = null;
	}

	public static void main(String[] args) {

		CVocabulary vocabulary = new CVocabulary();
		vocabulary.loadDic("D:\\R\\DragonSpide\\Spide\\Spide\\library\\近义词.dic");
		for (String word : vocabulary.getAllWords()) {
			System.out.println(vocabulary.getWordList(word).toString());
		}
	}
}
