package atguigu.utils;

import java.io.File;

public class DelTempDirUtils {
  public static void clean(String path) {
    File file = new File(path);
    delete(file);
  }

  private static void delete(File file) {
    if (file.isDirectory()) {
      File[] files = file.listFiles();
      if (files != null) {
        File[] var2 = files;
        int var3 = files.length;
        for (int var4 = 0; var4 < var3; var4++) {
          File file1 = var2[var4];
          delete(file1);
        }
      }
    }
    file.delete();
  }
}
