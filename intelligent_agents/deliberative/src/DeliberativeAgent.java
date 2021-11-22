import logist.agent.Agent;
import logist.behavior.DeliberativeBehavior;
import logist.plan.Plan;
import logist.simulation.Vehicle;
import logist.task.Task;
import logist.task.TaskDistribution;
import logist.task.TaskSet;
import logist.topology.Topology;
import logist.topology.Topology.City;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

// java -jar ../logist/logist.jar config/deliberative.xml deliberative-main

/**********************************************************************************
 *
 * An optimal planner for one vehicle.
 *
 **********************************************************************************/

@SuppressWarnings("unused")
public class DeliberativeAgent implements DeliberativeBehavior {

	enum Algorithm { BFS, ASTAR }
	
	// Environment
	Topology topology;
	TaskDistribution td;
	
	// The properties of the agent
	Agent agent;
	int capacity;

	// The planning class
	Algorithm algorithm;

	/******************************************************************************
	 *
	 *  Setup
	 *
	 ******************************************************************************/

	@Override
	public void setup(Topology topology, TaskDistribution td, Agent agent) {
		this.topology = topology;
		this.td = td;
		this.agent = agent;
		
		// Initialize the planner
		int capacity = agent.vehicles().get(0).capacity();
		String algorithmName = agent.readProperty("algorithm", String.class, "ASTAR");
		
		// Throws IllegalArgumentException if algorithm is unknown
		algorithm = Algorithm.valueOf(algorithmName.toUpperCase());
		
		// ...
	}

	/******************************************************************************
	 *
	 *  Choice of algorithms
	 *
	 ******************************************************************************/
	
	@Override
	public Plan plan(Vehicle vehicle, TaskSet tasks) {
		Plan plan;

		// Compute the plan with the selected algorithm.
		long start = System.currentTimeMillis();;
		switch (algorithm) {
		case ASTAR:
			plan = aStarPlan(vehicle, tasks);
			break;
		case BFS:
			plan = bfsPlan(vehicle, tasks);
			break;
		default:
			throw new AssertionError("Should not happen.");
		}		
		
		long time = System.currentTimeMillis() - start;
		System.out.println(time/1000);
		
		return plan;
	}

	/******************************************************************************
	 *
	 *  Naive planning
	 *
	 ******************************************************************************/

	private Plan naivePlan(Vehicle vehicle, TaskSet tasks) {
		City current = vehicle.getCurrentCity();
		Plan plan = new Plan(current);

		for (Task task : tasks) {
			// move: current city => pickup location
			for (City city : current.pathTo(task.pickupCity))
				plan.appendMove(city);

			plan.appendPickup(task);

			// move: pickup location => delivery location
			for (City city : task.path())
				plan.appendMove(city);

			plan.appendDelivery(task);

			// set current city
			current = task.deliveryCity;
		}
		return plan;
	}

	/******************************************************************************
	 *
	 *  Breath First Search Planning
	 *
	 ******************************************************************************/

	private Plan bfsPlan(Vehicle vehicle, TaskSet tasks) {
		
		State init = new State(vehicle, vehicle.getCurrentCity(), vehicle.getCurrentTasks(), tasks);
		Queue<State> q = new LinkedList<State>();
		HashMap<State, Double> visited = new HashMap<State, Double>();
		
		
		State finalS = null;
		
		q.add(init);
		
		State s = null;
		while(!q.isEmpty()) {
			
			s = q.poll();
			
			if(s.isFinal()) {
				if(finalS == null) {
					finalS = s;
				}else if(s.getgCost() < finalS.getgCost()) {
					finalS = s;
				}
			}
			
			else if(!visited.containsKey(s) || s.getgCost() < visited.get(s)) {
				visited.put(s, s.getgCost());
				q.addAll(s.getChildStates());
			}
			

		}
		
		System.out.println(finalS.getgCost());
		return finalS.generatePlanToThis();
	}

	/******************************************************************************
	 *
	 *  A* Planning
	 *
	 ******************************************************************************/

	private Plan aStarPlan(Vehicle vehicle, TaskSet tasks) {
		
		City current = vehicle.getCurrentCity();

		PriorityQueue<State> open = new PriorityQueue<State>(new fComparator());
		
		HashMap<State, Double> closed = new HashMap<State, Double>();
		
		State init = new State(vehicle, current, vehicle.getCurrentTasks(), tasks);

		
		open.add(init);
		
		
		int count = 0;

		State n = init;
		while(true){
			if (open.isEmpty()){
				System.out.println("Fail");
				break;
			}
			
			n = open.poll(); //get head
			
			if(n.isFinal()) {
				System.out.println(n.getgCost());
				return n.generatePlanToThis();
			}
			
			if(!closed.containsKey(n) || n.getgCost() < closed.get(n)) {
				closed.put(n, n.getgCost());
				open.addAll(n.getChildStates());
			}
			

			

			count++;
		}
		
		return null;
	}

	/******************************************************************************/

	@Override
	public void planCancelled(TaskSet carriedTasks) {
		
		if (!carriedTasks.isEmpty()) {
			// This cannot happen for this simple agent, but typically
			// you will need to consider the carriedTasks when the next
			// plan is computed.
		}
	}
}
