package project1.facA;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class Product2QuantityPair {

	private Text product;
	private IntWritable quantity;
	
	public Product2QuantityPair() {}
	
	public Product2QuantityPair(Text product, IntWritable quantity) {
		this.product=product;
		this.quantity=quantity;
	}
	
	public Text getProduct() {
		return product;
	}
	public void setProduct(Text product) {
		this.product = product;
	}
	public IntWritable getQuantity() {
		return quantity;
	}
	public void setQuantity(IntWritable quantity) {
		this.quantity = quantity;
	}
	
	public int hashcode() {
		return product.hashCode();
	}
	
	public boolean equals(Object o) {
		if (o instanceof Product2QuantityPair) {
            Product2QuantityPair pair = (Product2QuantityPair) o;
            return (product.equals(pair.getProduct())
                    );
        }
        return false;
	}

	public String toString(){
		return product.toString()+" "+ quantity.toString();
	}
	
}
