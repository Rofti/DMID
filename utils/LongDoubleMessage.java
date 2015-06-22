package org.apache.giraph.examples.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Simple message that contains the source vertex id and a double value.
 */
public class LongDoubleMessage implements Writable {
		
        /** Source vertex id */
        public long sourceVertexId;

		/** Value */
        public double value;
        
        public LongDoubleMessage() {}
        
        public LongDoubleMessage(long sourceVertexId, double value) {
            this.sourceVertexId = sourceVertexId;
            this.value = value;
        }
        
        @Override
        public void readFields(DataInput input) throws IOException {
            sourceVertexId = input.readLong();
            value = input.readDouble();
        }
        @Override
        public void write(DataOutput output) throws IOException {
            output.writeLong(sourceVertexId);
            output.writeDouble(value);
        }
        @Override
        public String toString() {
            return "(sourceVertexId=" + sourceVertexId + ",value=" + value + ")";
        }
        
        public long getSourceVertexId() {
			return sourceVertexId;
		}

		public void setSourceVertexId(long sourceVertexId) {
			this.sourceVertexId = sourceVertexId;
		}

		public double getValue() {
			return value;
		}

		public void setValue(double value) {
			this.value = value;
		}
    }

