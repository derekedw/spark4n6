package com.edwardsit.spark4n6.piggybank.evaluation.hash;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;

import org.apache.commons.codec.binary.Hex;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;

/**
 * This Apache Pig User-Defined Function (UDF) wraps 
 * {@link java.security.MessageDigest} to implement cryptographic hash 
 * functions like MD5 and SHA-1.  Hashes using any supported algorithm can
 * be generated. 
 * 
 * If you want to use these hash functions on a target that spans more than 
 * one {@link org.apache.pig.data.Tuple} record, you will have to first GROUP 
 * the records, then ORDER them, because 
 * 1. Pig relations, by default, don't guarantee order unless 
 * immediately preceded by the ORDER command, and
 * 2. Pig aggregate functions' boundaries are set by the GROUP command.
 * 
 * This gives a script of the form:   
 * <pre>
 * {@code
 *		-- Register the jar from this project
 *		REGISTER forn6piggybank_2.9.2-1.0.jar
 *		
 *		-- Define the UDFs for this project
 *		DEFINE SequenceFileLoader com.edwardsIT.forn6.piggybank.storage.SequenceFileLoader();
 *		DEFINE MD5 com.edwardsIT.forn6.piggybank.evaluation.hash.MD5();
 *		DEFINE SHA1 com.edwardsIT.forn6.piggybank.evaluation.hash.SHA1();
 *		DEFINE Hash com.edwardsIT.forn6.piggybank.evaluation.hash.Hash();
 *		
 *		-- Load the sequence file
 *		BLK = LOAD '/tmp/com.edwardsit.forn6.mediaimageimportertest6965862068571529225.seq'     
 *		        USING SequenceFileLoader()
 *		        as (key: long, value: bytearray);
 *		BLK_GROUP = GROUP BLK ALL;
 *		HASHES = FOREACH BLK_GROUP {
 *		                ORDERED = ORDER BLK by key;
 *		                GENERATE MD5(ORDERED.value), 
 *		                        SHA1(ORDERED.value), 
 *		                        Hash('SHA-256', ORDERED.value);
 *		};
 * }
 * </pre>
 * @author Derek derekedw@yahoo.com
 */
public class Hash extends EvalFunc<String>
{
	/**
	 * @see org.apache.pig.EvalFunc#exec(org.apache.pig.data.Tuple)
	 * The first argument in scripts must be a string, the 
	 * {@link java.security.MessageDigest} algorithm to calculate, and the 
	 * second is the field containing the data to be hashed.
	 */
	@Override
	public String exec(Tuple input) throws IOException {
		String algo = null;
		DataBag bag = null;
		Tuple t = null;
		MessageDigest md = null;
		if (input.get(0) instanceof String) {
			try {
				algo = (String) input.get(0);
				md = MessageDigest.getInstance(algo);
			} catch (NoSuchAlgorithmException e) {
				throw new IOException("No such algorithm '" + algo + "'", e);
			}
		}
		if (input.get(1) instanceof DataBag)
			bag = (DataBag)input.get(1);
		if (algo == null || bag == null)
			throw new IOException(this.getClass().getName() + "(algorithm: chararray, dataField: bag)");
		for (Iterator<Tuple> it = bag.iterator(); it.hasNext();) {
			t = it.next();
			if (t != null && t.size() > 0 && t.get(0) != null && 
					(t.get(0)) instanceof DataByteArray) 
			{
				md.update(((DataByteArray) t.get(0)).get());
			}
		}
		return Hex.encodeHexString(md.digest());
	}
}
