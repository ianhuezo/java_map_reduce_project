import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


public class Article implements Writable {

	private Text Category, Sender, Affiliation, Subject, Body;
	
	public Article(String content, String filePath) {
		this();
		String cat = filePath.split("\\/")[filePath.split("\\/").length - 2];
		String subject = "", body = "", org = "", topic = "", from = "";
		String [] lines = content.split("\\n");
		for(String line : lines) {
	    	if(line.startsWith("From:")) {
	    		from = line.replace("From:", "").trim();
	    	} else if(line.startsWith("Subject:")) {
	    		subject = line.replace("Subject:", "").trim();
	    	} else if(line.startsWith("Organization:")) {
	    		org = line.replace("Organization:", "").trim();
	    	} else if(line.contains(":")) {
	    		continue;
	    	} else if(line.trim().startsWith(">")) {
	    		continue;
	    	} else if(!line.matches("^[a-zA-Z\\-0-9]+:.*$")){
	    		body += line + " ";
	    	} 
	    }
		body = body.replaceAll("\\s+", " ");
		Subject.set(subject);
		Body.set(body);
		Affiliation.set(org);
		Sender.set(from);
		Category.set(cat);
	}
	
	public Article() {
		Category = new Text("");
		Sender = new Text("");
		Affiliation = new Text("");
		Subject = new Text("");
		Body = new Text("");
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		Category.readFields(in);
		Sender.readFields(in);
		Affiliation.readFields(in);
		Subject.readFields(in);
		Body.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Category.write(out);
		Sender.write(out);
		Affiliation.write(out);
		Subject.write(out);
		Body.write(out);
		
	}
	
	public String toString() {
		return Category.toString() + "\t" + Sender.toString() + "\t" + Subject.toString() + "\t" + Body.toString() + "\t" + Affiliation.toString();
	}
}