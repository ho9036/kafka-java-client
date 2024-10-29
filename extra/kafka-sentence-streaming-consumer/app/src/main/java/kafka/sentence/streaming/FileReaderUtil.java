package kafka.sentence.streaming;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class FileReaderUtil {
        public static List<String> readSentencesFromResources(String fileName) {
        List<String> sentences = new ArrayList<>();

        // 리소스 폴더에서 파일을 가져옴
        try (InputStream is = FileReaderUtil.class.getClassLoader().getResourceAsStream(fileName);
             BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {

            String line;
            while ((line = reader.readLine()) != null) {
                sentences.add(line);
            }
        } catch (IOException e) {
        } catch (NullPointerException e) {
            System.err.println("파일을 찾을 수 없습니다: " + fileName);
        }

        return sentences;
    }
}
