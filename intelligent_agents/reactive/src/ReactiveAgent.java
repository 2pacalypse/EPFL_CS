import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Random;


import logist.simulation.Vehicle;
import logist.agent.Agent;
import logist.behavior.ReactiveBehavior;
import logist.plan.Action;
import logist.plan.Action.Move;
import logist.plan.Action.Pickup;
import logist.task.Task;
import logist.task.TaskDistribution;
import logist.topology.Topology;
import logist.topology.Topology.City;


public class ReactiveAgent implements ReactiveBehavior{
    private Random random;
    private double pPickup;
    private int numActions;
    private Agent myAgent;
    private Topology topology;
	private TaskDistribution td;
    
    //this is the heart of the program
    //HashMap that returns State given (City,City).
    //HashMap can take null keys so it's great
 	private HashMap<City, HashMap<City, State>> states = new HashMap<>();

	private static HashMap<State, Double> V = new HashMap<>();
	private static HashMap<State, HashMap<PotentialAction, Double>> Q = new HashMap<>();



	/*******************************************************************************
     *
     * 	Setup of the agent
     *
     ******************************************************************************/

    @Override
    public void setup(Topology topology, TaskDistribution td, Agent agent) {

		this.topology = topology;
		this.td = td;
        // Reads the discount factor from the agents.xml file.
        // If the property is not present it defaults to 0.95
        Double discount = agent.readProperty("discount-factor", Double.class, 0.95);

        this.random = new Random();
        this.pPickup = discount;
        this.numActions = 0;
        this.myAgent = agent;
        
        //populate the hashtable with all the possible states
        for(City c1: topology.cities()) {
			HashMap<City, State> nested = new HashMap<City, State>();
			for(City c2: topology.cities()) {
				if (c1 == c2) c2 = null;
				State s = new State(c1, c2);
				nested.put(c2, s);
			}
			states.put(c1, nested);
		}

		/*

		Generation of the list of all the possible states

		 */

		List<State> stateList = new ArrayList<State>();
		for (City city : topology.cities()){
			stateList.add(new State(city, null));
			for (City destination : topology.cities()){
				if (destination != city){
					stateList.add(new State(city, destination));
				}
			}
		}

		/*

		Generation of the list of the Transition table T(s, a, s')

		 */

		TransitionTable transitionTable = new TransitionTable(stateList, topology, td);


		/*

		For each vehicle we need to calculate the reward because they don't have the same caracteristics

		 */





		/*

		Initialisation of V and Q

		 */
		for (State state : stateList){
			V.put(state, 0.0);
			Q.put(state, new HashMap<PotentialAction, Double>());
			for (PotentialAction a : state.actions()){
				Q.get(state).put(a, 0.0);
			}
		}
		for (Vehicle vehicle : agent.vehicles()){


			while (true){
				double err = 0;
				for (State s : stateList){
					for (PotentialAction a: s.actions()){

						double sum = 0;
						for (State s_prime : stateFromAction(a)){
							sum += transitionTable.getProbability(s, a, s_prime)*V.get(s_prime);

						}
						sum *= discount;
						System.out.println(sum);
						Q.get(s).put(a, sum + getReward(s, a, vehicle));

						//Q(s,a)←R(s,a)+γ􏰃_s′∈S T(s,a,s′)·V(s′)


					}

					//V (S) ← max_a Q(s, a)
				}

				break;
			}
		}



		// Value iteration
        while(true) {
			double err = 0;

			for(City c1: topology.cities()) {
				for(City c2: topology.cities()) {

					if (c1 == c2) c2 = null;

					// For each state s
					State s = states.get(c1).get(c2);
					for(int a: s.allowedActions) {
						//for each action a
						int r = (a == 0) ? td.reward(c1, c2) : 0; // remember a=0 means deliver, otherwise refuse
						double q = r;


						City newStateX = (a == 0) ? c2: c1.neighbors().get(a - 1);
						for(City newStateY: topology.cities()) {
							newStateY = (newStateX == newStateY) ? null : newStateY;
							//for each possible new state

							double p = td.probability(newStateX, newStateY); //probability of this transition
							double futureReward = states.get(newStateX).get(newStateY).v; //future reward

							q += discount*p*futureReward;
						}

						if(q > s.v) {
							err += Math.abs(q - s.v);
							s.v = q;
							s.bestAction = a;
						}
					}
				}
			}
			if(err < 1.0e-6) {
				break;
			}
		}

    }

    /*******************************************************************************
     *
     * 	Action of the agent
     *
     ******************************************************************************/

    @Override
    public Action act(Vehicle vehicle, Task availableTask) {
        Action action;
        City currentCity = vehicle.getCurrentCity();

        if (availableTask == null || random.nextDouble() > pPickup) {
            State s = states.get(currentCity).get(null);
			int best = s.bestAction;
			action = new Move(currentCity.neighbors().get(best - 1));
        } else {
            State s = states.get(currentCity).get(availableTask.deliveryCity);
			int best = s.bestAction;
			action = (best == 0) ? new Pickup(availableTask) : new Move(currentCity.neighbors().get(best - 1));

        }

        if (numActions >= 1) {
            System.out.println("The total profit after "+numActions+" actions is "+myAgent.getTotalProfit()+" (average profit: "+(myAgent.getTotalProfit() / (double)numActions)+")");
        }
        numActions++;

        return action;
    }

	public List<State> stateFromAction(PotentialAction action){
		List<State> stateList = new ArrayList<>();
		stateList.add(new State(action.destination, null));
		for (City newDestination : topology.cities()){
			if (newDestination != action.destination){
				stateList.add(new State(action.destination, newDestination));
			}
		}
		return stateList;
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

/*
	public List<State> stateListFromCity(City city){
		List<State> stateList = new ArrayList<State>();
		stateList.add(new State(city, null));
		for (City destination : topology.cities()){
			stateList.add(new State(city, null));
		}
	}*/
    /*******************************************************************************
     *
     * 	Getters and Setters
     *
     ******************************************************************************/


    /******************************************************************************/
}
