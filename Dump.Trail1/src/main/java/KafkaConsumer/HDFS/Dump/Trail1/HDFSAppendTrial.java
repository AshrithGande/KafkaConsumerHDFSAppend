package KafkaConsumer.HDFS.Dump.Trail1;

import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSAppendTrial {
	public FileSystem configureFileSystem(String coreSitePath, String hdfsSitePath) {  // HDFS FileSystem is configured here
  	    FileSystem fileSystem = null;
  	    try {
  	        Configuration conf = new Configuration();
  	        conf.setBoolean("dfs.support.append", true);
  	        Path coreSite = new Path(coreSitePath);
  	        Path hdfsSite = new Path(hdfsSitePath);
  	        conf.addResource(coreSite);
  	        conf.addResource(hdfsSite);
  	        fileSystem = FileSystem.get(conf);
  	    } catch (IOException ex) {
  	        System.out.println("Error occurred while configuring FileSystem");
  	    }
  	    return fileSystem;
  	}
  	public String appendToFile(FileSystem fileSystem, String content, String dest) throws IOException { // Here data is appended to file location specified
        String editedContent = content+"\n";
  	    Path destPath = new Path(dest);
  	    if (!fileSystem.exists(destPath)) {
  	        System.err.println("File doesn't exist");
  	        return "Failure";
  	    }

  	    Boolean isAppendable = Boolean.valueOf(fileSystem.getConf().get("dfs.support.append"));

  	    if(isAppendable) {
  	        FSDataOutputStream fs_append = fileSystem.append(destPath);
  	        PrintWriter writer = new PrintWriter(fs_append);
  	        writer.append(editedContent);
  	        writer.flush();
  	        fs_append.hflush();
  	        writer.close();
  	        fs_append.close();
  	        return "Success";
  	    }
  	    else {
  	        System.err.println("Please set the dfs.support.append property to true");
  	        return "Failure";
  	    }
  	}
  	public void closeFileSystem(FileSystem fileSystem){
        try {
            fileSystem.close();
        }
        catch (IOException ex){
            System.out.println("----------Could not close the FileSystem----------");
        }
    }
  	
    

}
