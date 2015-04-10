import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Text;


public class TestDate {

	public static void main(String[] args) throws ParseException {
		/*Text date1 = new Text("2014-4-29");
		Text date2 = new Text("2015-3-29");
		SimpleDateFormat myFormat = new SimpleDateFormat("yyyy-mm-dd");
		int res = myFormat.parse(date1.toString()).compareTo(myFormat.parse(date2.toString()));
		System.out.println(res);*/
		
		
		/*String tryDate = "2015-2-27";
		boolean result = isFirstTrimester2015(tryDate);
		System.out.println(result);*/
		
		Text date1 = new Text("2015-2");
		Text date2 = new Text("2015-3");
		System.out.println(date1.hashCode());
		System.out.println(date2.hashCode());
		System.out.println(date1.compareTo(date2));
		System.out.println(date1.equals(date2));

}
	
	public static boolean isFirstTrimester2015(String date) {
		SimpleDateFormat myFormat = new SimpleDateFormat("yyyy-MM-dd");
		String firstDay = "2014-12-31";
		String lastDay = "2015-3-28";
		Date firstDate=null;
		Date lastDate = null;
		Date toVerify = null;
		try {
			 firstDate = myFormat.parse(firstDay);
			 lastDate = myFormat.parse(lastDay);
			 toVerify = myFormat.parse(date);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return  toVerify.after(firstDate) && toVerify.before(lastDate);
		
	}
	
}
