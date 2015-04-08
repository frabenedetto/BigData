import java.util.StringTokenizer;


public class Test {

	public static void main(String[] args){
	String str = "20150321,uova,latte,pane,vino\n20150218,pesce,pane,insalata,formaggio ";
	//System.out.println(str);
	str = str.replaceAll("\\d+(?:[.,]\\d+)*\\s*", "");
	//System.out.println(str);
	StringTokenizer tokenizer = new StringTokenizer(str, ",", false);
	while
		(tokenizer.hasMoreTokens())
	{
		System.out.println(tokenizer.nextToken().toString());
	}
}
}
