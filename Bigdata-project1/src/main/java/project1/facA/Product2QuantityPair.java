package project1.facA;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Product2QuantityPair implements WritableComparable<Product2QuantityPair>{

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

	public void readFields(DataInput in) throws IOException {
		product = new Text(in.readUTF());
		quantity = new IntWritable(Integer.parseInt(in.readUTF()));
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(product.toString());
		out.writeUTF(quantity.toString());
	}

	public int compareTo(Product2QuantityPair arg0) {
		int cmp = this.product.compareTo(arg0.getProduct());
		if(cmp==0) {
			return this.quantity.compareTo(arg0.getQuantity());
		}
		return cmp;
	}
	
}
