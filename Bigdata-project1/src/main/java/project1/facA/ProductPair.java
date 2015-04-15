package project1.facA;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class ProductPair implements WritableComparable<ProductPair>{

	private Text productLeft,productRight;
	
	
	public ProductPair(){}
	
	public ProductPair(Text productLeft, Text productRight){
		this.productLeft = productLeft;
		this.productRight = productRight;
	}
	
	public void set(Text productLeft, Text productRight){
		this.productLeft = productLeft;
		this.productRight = productRight;
	}
	
	public Text getProductLeft() {
		return productLeft;
	}
	

	public void setProductLeft(Text productLeft) {
		this.productLeft = productLeft;
	}

	public Text getProductRight() {
		return productRight;
	}

	public void setProductRight(Text productRight) {
		this.productRight = productRight;
	}

	public void readFields(DataInput in) throws IOException {
		productLeft = new Text(in.readUTF());
        productRight = new Text(in.readUTF());		
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(productLeft.toString());
        out.writeUTF(productRight.toString());		
	}

	public int compareTo(ProductPair arg0) {
		int cmp = this.productLeft.compareTo(arg0.getProductLeft());
		if(cmp!=0) {
			return cmp;
		}
		return this.productRight.compareTo(arg0.getProductRight());
	}
	
	public int hashcode() {
		return productLeft.hashCode()+productRight.hashCode();
	}
	
	public boolean equals(Object o) {
		if (o instanceof ProductPair) {
            ProductPair pair = (ProductPair) o;
            return (productLeft.equals(pair.getProductLeft())
                    && productRight.equals(pair.getProductRight()));
        }
        return false;
	}

	public String toString(){
		return productLeft.toString()+" "+ productRight.toString();
	}
}
