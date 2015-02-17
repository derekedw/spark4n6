package com.edwardsit.spark4n6.piggybank.evaluation.hash;

import java.io.IOException;
import org.apache.pig.data.*;

/**
 * Apache Pig User-Defined Function (UDF) that calculates MD5 hash.  It is
 * essentially
 * {@code
 * Hash("MD5", myData);
 * }
 * @author Derek derekedw@yahoo.com
 */
public class MD5 extends Hash {
	@Override
	public String exec(Tuple input) throws IOException {
		String algo = "MD5";
		DataBag bag = (DataBag)input.get(0);
		Tuple t = TupleFactory.getInstance().newTuple(2);
		t.set(0, algo);
		t.set(1, bag);
		return super.exec(t);
	}
}
