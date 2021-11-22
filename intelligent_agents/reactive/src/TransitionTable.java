import java.util.Iterator;
import java.util.List;
import java.util.HashMap;

import logist.plan.Action;
import logist.task.TaskDistribution;
import logist.topology.Topology;
import logist.topology.Topology.City;

/*
The transition table is a table of probabilities that's map a state and action to a new state.

So given a state and an action it must be able to give us the probability to be on a given new state latter.
In other word it is a 3D array where each cases are probabilies, and the axes are State s, Action a, State s_prime.

The class TransitionTable  gives us an interface to this array

 */
public class TransitionTable{

    /*******************************************************************************
     *
     * 	Creation of the Transition Table
     *
     ******************************************************************************/

    private HashMap<State, HashMap<PotentialAction, HashMap<State, Double>>> T;

    public TransitionTable(List<State> stateList, Topology t,  TaskDistribution td) {
        T = new HashMap<State, HashMap<PotentialAction, HashMap<State, Double>>>();
        for (State s : stateList){
            HashMap<PotentialAction, HashMap<State, Double>> nested1 = new HashMap<>();
            for (PotentialAction a: s.actions()){
                HashMap<State, Double> nested2 = new HashMap<>();
                double sum = 0;
                for (City newDestination : t) {
                    if (a.destination != newDestination){
                        State s_prime = new State(a.destination, newDestination);
                        nested2.put(s_prime, td.probability(s_prime.from, s_prime.to));
                        sum += td.probability(s_prime.from, s_prime.to);
                    }
                }
                State s_prime = new State(a.destination, null);
                nested2.put(s_prime, 1 - sum);
                nested1.put(a, nested2);
            }
            T.put(s,nested1);
        }
    }

    /*******************************************************************************
     *
     * 	Getter
     *
     ******************************************************************************/

    public Double getProbability(State s, PotentialAction a, State s_prime){
        //System.out.println("getProbability => " + s + " " + " " + a + " " + " " + s_prime);
        return T.get(s).get(a).get(s_prime);
    }

}
