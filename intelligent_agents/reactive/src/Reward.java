import logist.simulation.Vehicle;
import logist.task.TaskDistribution;
import logist.topology.Topology;
import logist.topology.Topology.City;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Reward {
    /*******************************************************************************
     *
     * 	Creation of the Reward Table
     *
     ******************************************************************************/

    Map<State, Map<PotentialAction,Double>> reward;

    private Topology topology;
    private TaskDistribution td;
    public Reward(List<State> stateList, TaskDistribution td, Vehicle vehicle) {
       this.topology = topology;
       this.td = td;
       /*
       for (State s : stateList){
           Map<PotentialAction,Double> nested = new HashMap<PotentialAction,Double>();
           for (PotentialAction a: s.actions()){
               double r;
               if (a.isADelivery()){
                    r = td.reward(s.from, s.to) - s.from.distanceTo(s.to) * vehicle.costPerKm();
               } else {
                   r = - s.from.distanceTo(a.destination) * vehicle.costPerKm();
               }
               nested.put(a, r);
           }
           reward.put(s,nested);
       }*/
    }

    double getReward(State s, PotentialAction a, Vehicle vehicle){
        double r;
        if (a.isADelivery()){
            r = td.reward(s.from, s.to) - s.from.distanceTo(s.to) * vehicle.costPerKm();
        } else {
            r = - s.from.distanceTo(a.destination) * vehicle.costPerKm();
        }
        return r;
    }
}
