import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.io.Writable;

public class FakeInputSplit extends InputSplit implements Writable {

    private int numPoints;

    public FakeInputSplit() {}

    public FakeInputSplit(int numPoints) {
        this.numPoints = numPoints;
    }

    @Override
    public long getLength() {
        return numPoints;
    }

    @Override
    public String[] getLocations() {
        return new String[0];
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(numPoints);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        numPoints = in.readInt();
    }

    public int getNumPoints() {
        return numPoints;
    }
}
