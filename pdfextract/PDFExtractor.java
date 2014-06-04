import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;

public class PDFExtractor {
	public static void main(String[] argv) {
		int i = 1;
		boolean flag = false;
		String usage = "Please enter the correct number of arguments. <FileName> <Crawl Directory> <Output Directory>";
		if (argv.length != 2) {
			System.out.println("usage:" + usage);
			return;
		}
		try {
			Configuration configuration = NutchConfiguration.create();
			FileSystem fileSystem = FileSystem.get(configuration);
			HashSet<String> fileNames = new HashSet<String>();
			File file = new File(argv[0]);
			File[] fileArray = file.listFiles();
			if (fileArray != null) {
				for (File file1 : fileArray) {
					Path path = new Path(file1.getAbsolutePath(),
							Content.DIR_NAME + "/part-00000/data");
					SequenceFile.Reader reader = new SequenceFile.Reader(
							fileSystem, path, configuration);
					Text text = new Text();
					Content content = new Content();
					while (reader.next(text, content)) {
						if (text.equals(new Text(argv[0]))) {
							System.out.write(content.getContent(), 0,
									content.getContent().length);
							break;
						}
						if (content.getContentType().equalsIgnoreCase(
								"application/pdf")) {
							String fileName = content.getUrl().toString();
							if (fileName.contains("/at_download/file"))
								fileName = fileName.substring(0,
										fileName.indexOf("/at_download/file"));
							fileName = fileName.substring(
									fileName.lastIndexOf("/") + 1,
									fileName.length());
							if (fileName.contains("index.html"))
								fileName = fileName.substring(0,
										fileName.indexOf(".html"));
							fileName = fileName.substring(fileName.lastIndexOf("/") + 1,
									fileName.length());
							fileName = fileName.replaceAll("%20", " ");
							if(fileName.contains(".pdf"))
								fileName = fileName.replaceAll(".pdf", "");
							if (!fileNames.contains(fileName)) {

								BufferedOutputStream bufferedOutput = null;							
								bufferedOutput = new BufferedOutputStream(
										new FileOutputStream(argv[1] + "/"
												+ fileName + ".pdf"));
								bufferedOutput.write(content.getContent());
								System.out.println((i++) + ". " + fileName
										+ ".pdf created successfully.");
								fileNames.add(fileName);
								bufferedOutput.close();
							}
						}
					}
					reader.close();
				}
			}
			System.out.println("Total number of pdf files created :" + (i - 1));
			flag = true;
			fileSystem.close();
		} catch (IOException e) {
			if(flag)
				System.err.println(e.toString());
			else
				System.out.println("Total number of pdf files created :" + (i - 1));
		} catch (Exception e) {
			System.err.println(e.toString());
		}
	}
}
