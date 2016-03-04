/*
 * Copyright 2015-2016 Derek Edwards
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
