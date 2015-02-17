package com.edwardsit.spark4n6.piggybank.evaluation.hash;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;

/**
 * Apache Pig User-Defined Function (UDF) that calculates a SHA-1 hash.  It is
 * essentially
 * {@code
 * Hash("SHA1", myData);
 * }
 * @author Derek derekedw@yahoo.com
 */
public class SHA1 extends Hash {
	@Override
	public String exec(Tuple input) throws IOException {
		String algo = "SHA1";
		DataBag bag = (DataBag) input.get(0);
		Tuple t = TupleFactory.getInstance().newTuple(2);
		t.set(0, algo);
		t.set(1, bag);
		return super.exec(t);
	}
}
