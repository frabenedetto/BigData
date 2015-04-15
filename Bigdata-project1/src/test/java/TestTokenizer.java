import java.util.StringTokenizer;


public class TestTokenizer {

	public static void main(String[] args){
	String str = "uova pane   34";//\n2015-02-18,pesce,pane,insalata,formaggio ";
	//System.out.println(str);
	//str = str.replaceAll("\\d+(?:[.,]\\d+)*\\s*", "");
	//System.out.println(str);
	StringTokenizer tokenizer = new StringTokenizer(str);
	while
		(tokenizer.hasMoreTokens())
	{
		System.out.println(tokenizer.nextToken().toString());
	}
}
}
