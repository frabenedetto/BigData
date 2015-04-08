import java.util.StringTokenizer;


public class Test {

	public static void main(String[] args){
	String str = "2015-03-21,uova,latte,pane,vino";//\n2015-02-18,pesce,pane,insalata,formaggio ";
	//System.out.println(str);
	str = str.replaceAll("\\d+(?:[.,]\\d+)*\\s*", "");
	//System.out.println(str);
	StringTokenizer tokenizer = new StringTokenizer(str, ",", false);
	tokenizer.nextToken();
	while
		(tokenizer.hasMoreTokens())
	{
		System.out.println(tokenizer.nextToken().toString());
	}
}
}
