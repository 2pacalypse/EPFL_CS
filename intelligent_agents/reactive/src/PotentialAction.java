import logist.topology.Topology.City;

import java.util.ArrayList;
import java.util.List;

/*
A Potential Action as a type : deliver or move.

We can get the reward form it
*/

public class PotentialAction {
    private int type;
    public City destination;
    private int reward;

    public PotentialAction(int type, City destination){
        this.type = type;
        this.destination = destination;
    }

    boolean isADelivery(){
        if (type == 1){
            return true;
        }
        return false;
    }

    public int hashCode(){
        String name = Integer.toString(type) + destination.name;
        return name.hashCode();
    }

    public String toString() {
        return "type " + type +
                ", to " + destination;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PotentialAction) {
            return ((PotentialAction) obj).toString().equals(this.toString());
        }
        return false;
    }
}
