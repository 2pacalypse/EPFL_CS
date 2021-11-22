import java.util.List;
import java.util.ArrayList;
import logist.topology.Topology.City;


/**
 * 
 * Actions are defined such that if there are
 * n cities in the topology, then
 *  
 * A_0 = go to toCity (accept the task).
 * A_1 = Go to first neighbor--neighbors[0] (refuse the task)
 * A_2 = go to second neighbor--neighbors[1] (refuse the task)
 * .
 * .
 * .
 * A_n = go to nth neighbor (refuse the task)
 * allowedActions is an array containing the valid action indices.
 * i.e. it contains j for each A_j.
 */

public class State {
	// for the actions:
	// 1,2,..n, means go to neighbor[n-1] by refusing the task.
	// 0 means deliver the task specified in the state.
	
	double v; //accumulated value
	int bestAction;
	public City from;
	public City to;
	List<Integer> allowedActions;

	State(City from, City to){
		this.from = from;
		this.to = to;

		findAllowedActions(from, to);
		v = 0;
		bestAction = 0;
	}

	public List<PotentialAction> actions(){
		List<PotentialAction> potentialActions = new ArrayList<PotentialAction>();
		if (to != null) {
			potentialActions.add(new PotentialAction(0, to));
		}
		for(City destination : from.neighbors()) {
			potentialActions.add(new PotentialAction(1, destination));

		}
		return potentialActions;
	}

	public City getCurrentCity(){
		return from;
	}





	void findAllowedActions(City from, City to){
		allowedActions = new ArrayList<Integer>();
		if(to != null) {
			allowedActions.add(0);
		}
		for(int i = 0; i < from.neighbors().size(); i++) {
			allowedActions.add(i + 1);
		}
	}

	@Override
	public String toString() {
	    if (to != null){
            return "State{" +
                    "from=" + from +
                    ", to=" + to +
                    '}';
        }
		else{
            return "State{" +
                    "from=" + from +
                    ", to=null" +
                    '}';
        }
	}

	@Override
	public int hashCode(){
        String name;
	    if (to == null){
            name = from.name;
        }else{
            name = from.name + to.name ;
        }
        //System.out.println(name);
		return name.hashCode();
	}

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof State) {
            return ((State) obj).toString().equals(this.toString());
        }
        return false;
    }
}
